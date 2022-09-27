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
    l1_submission_batch_index AS l1_submission_batch_index,
    l1_submission_tx_hash,
    state_batch_index AS l1_state_root_batch_index,
    state_tx_hash AS l1_state_root_tx_hash,
    cumulative_Gas_Used,
    input_data,
    status,
    tx_json
FROM
    {{ ref('silver__transactions') }}
    LEFT JOIN {{ ref('bronze__state_hashes') }}
    ON block_number BETWEEN state_min_block
    AND state_max_block
    LEFT JOIN {{ ref('bronze__submission_hashes') }}
    ON block_number BETWEEN sub_min_block
    AND sub_max_block
