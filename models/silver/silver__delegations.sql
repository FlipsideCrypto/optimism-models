{{ config(
    materialized = 'incremental',
    unique_key = "tx_hash",
    cluster_by = ['block_timestamp::DATE']
) }}

SELECT 
    block_number, 
    block_timestamp,
    tx_hash,
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
        CONCAT('0x', SUBSTR(tx_json :receipt :logs[0] :topics[1] :: STRING, 27, 40)) 
    ELSE 
        tx_json :receipt :logs[0] :decoded :inputs :toDelegate :: STRING
    END AS to_delegate, 
    CASE WHEN delegation_type = 'Re-Delegation' THEN 
        CONCAT('0x', SUBSTR(tx_json :receipt :logs[0] :topics[2] :: STRING, 27, 40))
    ELSE 
        NULL 
    END AS from_delegate, 
    eth_value, 
    gas_price,
    gas_used, 
    gas_limit, 
    tx_fee, 
    _inserted_timestamp
  FROM 
    {{ ref('silver__transactions') }}
  WHERE 
    origin_function_signature = '0x5c19a95c'
    AND to_address = '0x4200000000000000000000000000000000000042'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}