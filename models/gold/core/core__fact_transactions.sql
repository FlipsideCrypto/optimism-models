{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    from_address,
    to_address,
    origin_function_signature,
    VALUE,
    value_precise_raw,
    value_precise,
    tx_fee,
    tx_fee_precise,
    CASE 
        WHEN tx_status = 'SUCCESS' THEN TRUE 
        ELSE FALSE 
    END AS tx_succeeded, --new column
    tx_type,
    nonce,
    POSITION AS tx_position, --new column
    input_data,
    gas_price,
    gas_used,
    gas AS gas_limit,
    cumulative_gas_used,
    effective_gas_price,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    l1_gas_price,
    l1_gas_used,
    l1_fee_scalar,
    l1_fee_precise,
    l1_fee,
    r,
    s,
    v,
    COALESCE (
        transactions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash']
        ) }}
    ) AS fact_transactions_id,
    GREATEST(
        COALESCE(A.inserted_timestamp, '2000-01-01'), 
        COALESCE(b.inserted_timestamp, '2000-01-01'), --remove
        COALESCE(C.inserted_timestamp, '2000-01-01') --remove
    ) AS inserted_timestamp,
    GREATEST(
        COALESCE(A.modified_timestamp, '2000-01-01'), 
        COALESCE(b.modified_timestamp, '2000-01-01'), --remove
        COALESCE(C.modified_timestamp, '2000-01-01') --remove
    ) AS modified_timestamp,
    block_hash, --deprecate
    tx_status AS status, --deprecate
    POSITION, --deprecate
    l1_submission_batch_index, --deprecate, may build separate table
    l1_submission_tx_hash, --deprecate, may build separate table
    state_batch_index AS l1_state_root_batch_index, --deprecate, may build separate table
    state_tx_hash AS l1_state_root_tx_hash, --deprecate, may build separate table
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
    ) AS l1_submission_details, --deprecate, may build separate table
    deposit_nonce, --deprecate, may build separate table
    deposit_receipt_version --deprecate, may build separate table
FROM
    {{ ref('silver__transactions') }} A
    LEFT JOIN {{ ref('silver__state_hashes') }} --remove
    b USING (block_number)
    LEFT JOIN {{ ref('silver__submission_hashes') }} --remove
    C USING (block_number)
