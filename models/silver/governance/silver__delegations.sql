{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','governance', 'curated']
) }}

with base as (
    SELECT 
        block_number,
        block_timestamp,
        tx_hash,
        iff(tx_succeeded, 'SUCCESS', 'FAIL') AS status,
        event_name,
        origin_from_address,
        origin_to_address,
        decoded_log,
        decoded_log:delegate::string as delegate,
        try_to_number(decoded_log:newBalance::string) as new_balance,
        try_to_number(decoded_log:previousBalance::string) as previous_balance,
        decoded_log:delegator::string as delegator,
        decoded_log:fromDelegate::string as from_delegate,
        decoded_log:toDelegate::string as to_delegate
    FROM {{ ref('core__ez_decoded_event_logs') }}
    WHERE contract_address = '0x4200000000000000000000000000000000000042'
    AND event_name IN ('DelegateChanged', 'DelegateVotesChanged')
    AND tx_succeeded
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM {{ this }}
    )
    {% endif %}
), 
change_info as (
    select 
        block_number,
        tx_hash,
        delegator,
        from_delegate,
        to_delegate,
        case 
            when delegator = to_delegate and from_delegate = '0x0000000000000000000000000000000000000000' then 'First Time Delegator - Self Delegation'
            when delegator = to_delegate and from_delegate <> '0x0000000000000000000000000000000000000000' then 'Self Delegation'
            when delegator <> to_delegate and from_delegate = '0x0000000000000000000000000000000000000000' then 'First Time Delegator'
            else 'Re-Delegation'
        end as delegation_type
    from base 
    where event_name = 'DelegateChanged'
)
SELECT
    b0.block_number,
    b0.block_timestamp,
    b0.tx_hash,
    b0.status,
    b0.event_name,
    b1.delegator,
    b1.delegation_type,
    b1.to_delegate,
    b1.from_delegate,
    b0.delegate,
    b0.previous_balance as raw_previous_balance,
    b0.new_balance as raw_new_balance,
    COALESCE(raw_previous_balance / pow(10, 18), 0) AS previous_balance,
    COALESCE(raw_new_balance / pow(10, 18), 0) AS new_balance,
    modified_timestamp AS _inserted_timestamp,
    CONCAT(tx_hash :: STRING, '-', event_index :: STRING) AS _log_id,
    {{ dbt_utils.generate_surrogate_key(['tx_hash', 'event_index']) }} AS delegations_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
    FROM 
        base b0
    JOIN change_info b1 using (block_number, tx_hash)
    WHERE b0.event_name = 'DelegateVotesChanged'