{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    block_hash,
    tx_hash,
    nonce,
    POSITION,
    origin_function_signature,
    from_address,
    to_address,
    eth_value,
    tx_fee,
    gas_price,
    gas_limit,
    gas_used,
    l1_gas_price,
    l1_gas_used,
    l1_fee_scalar,
    l1_submission_batch_index,
    l1_submission_tx_hash,
    l1_state_root_batch_index,
    l1_state_root_tx_hash,
    cumulative_Gas_Used,
    input_data,
    status,
    tx_json
FROM
    {{ ref('silver__transactions') }}
