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
    VALUE,
    value_precise_raw,
    value_precise,
    tx_fee,
    tx_fee_precise,
    gas_price,
    effective_gas_price,
    gas AS gas_limit,
    gas_used,
    l1_gas_price,
    l1_gas_used,
    l1_fee_scalar,
    l1_fee_precise,
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
    v,
    tx_type,
    COALESCE (
        transactions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash']
        ) }}
    ) AS fact_transactions_id,
    GREATEST(COALESCE(A.inserted_timestamp, '2000-01-01'), COALESCE(b.inserted_timestamp, '2000-01-01'), COALESCE(C.inserted_timestamp, '2000-01-01')) AS inserted_timestamp,
    GREATEST(COALESCE(A.modified_timestamp, '2000-01-01'), COALESCE(b.modified_timestamp, '2000-01-01'), COALESCE(C.modified_timestamp, '2000-01-01')) AS modified_timestamp,
    deposit_nonce,
    deposit_receipt_version
FROM
    {{ ref('silver__transactions') }} A
    LEFT JOIN {{ ref('silver__state_hashes') }}
    b USING (block_number)
    LEFT JOIN {{ ref('silver__submission_hashes') }} C USING (block_number)
