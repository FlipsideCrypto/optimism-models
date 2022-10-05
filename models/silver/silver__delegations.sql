{{ config(
    materialized = 'incremental',
    unique_key = "_log_id",
    cluster_by = ['block_timestamp::DATE']
) }}

SELECT 
    t.block_number, 
    t.block_timestamp,
    t.tx_hash,
    status, 
    from_address AS delegator,
    CASE WHEN tx_json:receipt:logs[0]:decoded:inputs:fromDelegate :: STRING = '0x0000000000000000000000000000000000000000' AND tx_json :receipt :logs[0] :decoded :inputs :toDelegate :: STRING <> from_address THEN 
        'First Time Delegator'
    WHEN tx_json:receipt:logs[0]:decoded:inputs:fromDelegate::string = '0x0000000000000000000000000000000000000000' AND delegator = tx_json :receipt :logs[0] :decoded :inputs :toDelegate :: STRING THEN 
        'First Time Delegator - Self Delegation'
    WHEN delegator = tx_json :receipt :logs[0] :decoded :inputs :toDelegate :: STRING THEN 
        'Self-Delegation'
    ELSE 
        'Re-Delegation' 
    END AS delegation_type, 
    CASE WHEN delegation_type = 'Re-Delegation' THEN 
        l.event_inputs :delegate :: STRING 
    ELSE 
        tx_json :receipt :logs[0] :decoded :inputs :toDelegate :: STRING
    END AS to_delegate, 
    CASE WHEN delegation_type = 'Re-Delegation' THEN 
        CONCAT('0x', SUBSTR(tx_json :receipt :logs[0] :topics[2] :: STRING, 27, 40))
    ELSE 
        NULL 
    END AS from_delegate, 
    l.event_inputs :newBalance :: NUMBER AS raw_new_balance, 
    l.event_inputs :previousBalance :: NUMBER AS raw_previous_balance,
    t._inserted_timestamp, 
    l._log_id
FROM 
    {{ ref('silver__transactions') }} t

LEFT OUTER JOIN {{ ref('silver__logs')}} l 
ON t.tx_hash = l.tx_hash

WHERE 
    t.origin_function_signature = '0x5c19a95c'
    AND to_address = '0x4200000000000000000000000000000000000042'
    AND event_name = 'DelegateVotesChanged'

{% if is_incremental() %}
AND t._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
AND l._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}