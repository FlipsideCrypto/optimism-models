{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    A.block_number AS block_number,
    block_timestamp,
    block_hash,
    tx_hash,
    nonce,
    POSITION,
    origin_function_signature,
    from_address,
    to_address,
    VALUE AS eth_value,
    tx_fee,
    gas_price,
    effective_gas_price,
    gas AS gas_limit,
    gas_used,
    l1_gas_price,
    l1_gas_used,
    l1_fee_scalar,
    l1_fee,
    l1_submission_batch_index,
    l1_submission_tx_hash,
    state_batch_index AS l1_state_root_batch_index,
    state_tx_hash AS l1_state_root_tx_hash,
    OBJECT_CONSTRUCT(
        'l1_bedrock_state_batch_index',
        bedrock_state_batch_index,
        'l1_bedrock_state_batch_root',
        bedrock_state_batch_root,
        'l1_state_root_batch_index',
        state_batch_index,
        'l1_state_root_tx_hash',
        state_tx_hash,
        'l1_submission_batch_index',
        l1_submission_batch_index,
        'l1_submission_batch_root',
        l1_submission_batch_root,
        'l1_submission_tx_hash',
        l1_submission_tx_hash
    ) AS l1_submission_details,
    cumulative_gas_used,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    input_data,
    tx_status AS status,
    r,
    s,
    v
FROM
    {{ ref('silver__transactions') }} A
    LEFT JOIN {{ ref('silver__state_hashes') }}
    b
    ON A.block_number = b.block_number
    LEFT JOIN {{ ref('silver__submission_hashes') }} C
    ON A.block_number = C.block_number