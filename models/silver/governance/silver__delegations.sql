{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','governance', 'curated']
) }}

WITH base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        IFF(
            tx_succeeded,
            'SUCCESS',
            'FAIL'
        ) AS status,
        event_name,
        decoded_log,
        modified_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        contract_address = '0x4200000000000000000000000000000000000042'
        AND event_name IN (
            'DelegateChanged',
            'DelegateVotesChanged'
        )
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),
votes_changed AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        status,
        event_name,
        decoded_log :delegate :: STRING AS delegate,
        TRY_TO_NUMBER(
            decoded_log :newBalance :: STRING
        ) AS new_balance,
        TRY_TO_NUMBER(
            decoded_log :previousBalance :: STRING
        ) AS previous_balance,
        modified_timestamp
    FROM
        base
    WHERE
        event_name = 'DelegateVotesChanged'
),
change_info AS (
    SELECT
        block_number,
        tx_hash,
        decoded_log :delegator :: STRING AS delegator,
        decoded_log :fromDelegate :: STRING AS from_delegate,
        decoded_log :toDelegate :: STRING AS to_delegate,
        CASE
            WHEN delegator = to_delegate
            AND from_delegate = '0x0000000000000000000000000000000000000000' THEN 'First Time Delegator - Self Delegation'
            WHEN delegator = to_delegate
            AND from_delegate <> '0x0000000000000000000000000000000000000000' THEN 'Self Delegation'
            WHEN delegator <> to_delegate
            AND from_delegate = '0x0000000000000000000000000000000000000000' THEN 'First Time Delegator'
            ELSE 'Re-Delegation'
        END AS delegation_type
    FROM
        base
    WHERE
        event_name = 'DelegateChanged'
)
SELECT
    b0.block_number,
    b0.block_timestamp,
    b0.tx_hash,
    b0.status,
    b0.event_name,
    b1.delegator,
    b1.delegation_type,
    b0.delegate,
    b0.previous_balance AS raw_previous_balance,
    b0.new_balance AS raw_new_balance,
    COALESCE(raw_previous_balance / pow(10, 18), 0) AS previous_balance,
    COALESCE(raw_new_balance / pow(10, 18), 0) AS new_balance,
    b1.to_delegate,
    b1.from_delegate,
    modified_timestamp AS _inserted_timestamp,
    CONCAT(
        tx_hash :: STRING,
        '-',
        event_index :: STRING
    ) AS _log_id,
    {{ dbt_utils.generate_surrogate_key(['tx_hash', 'event_index']) }} AS delegations_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    votes_changed b0
    JOIN change_info b1 USING (
        block_number,
        tx_hash
    )
WHERE
    b0.event_name = 'DelegateVotesChanged'
