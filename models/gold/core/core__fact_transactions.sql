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
    eth_value_precise_raw,
    eth_value_precise,
    tx_fee,
    tx_fee_precise,
    gas_price,
    effective_gas_price,
    gas_limit,
    gas_used,
    l1_gas_price,
    l1_gas_used,
    l1_fee_scalar,
    l1_fee,
    l1_fee_precise,
    l1_submission_batch_index,
    l1_submission_tx_hash,
    l1_state_root_batch_index,
    l1_state_root_tx_hash,
    l1_submission_details,
    cumulative_gas_used,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    input_data,
    status,
    r,
    s,
    v,
    tx_type
FROM
    (
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
            VALUE AS eth_value,
            tx_fee,
            tx_fee_precise,
            gas_price,
            effective_gas_price,
            gas AS gas_limit,
            gas_used,
            l1_gas_price,
            l1_gas_used,
            l1_fee_scalar,
            IFF(LENGTH(l1_fee_precise_raw) > 18, LEFT(l1_fee_precise_raw, LENGTH(l1_fee_precise_raw) - 18) || '.' || RIGHT(l1_fee_precise_raw, 18), '0.' || LPAD(l1_fee_precise_raw, 18, '0')) AS rough_conversion_l1fee,
            IFF(
                POSITION(
                    '.000000000000000000' IN rough_conversion_l1fee
                ) > 0,
                LEFT(
                    rough_conversion_l1fee,
                    LENGTH(rough_conversion_l1fee) - 19
                ),
                REGEXP_REPLACE(
                    rough_conversion_l1fee,
                    '0*$',
                    ''
                )
            ) AS l1_fee_precise,
            (l1_fee_precise_raw / pow(10, 18)) :: FLOAT AS l1_fee,
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
            to_varchar(
                TO_NUMBER(REPLACE(DATA :value :: STRING, '0x'), REPEAT('X', LENGTH(REPLACE(DATA :value :: STRING, '0x'))))
            ) AS eth_value_precise_raw,
            IFF(LENGTH(eth_value_precise_raw) > 18, LEFT(eth_value_precise_raw, LENGTH(eth_value_precise_raw) - 18) || '.' || RIGHT(eth_value_precise_raw, 18), '0.' || LPAD(eth_value_precise_raw, 18, '0')) AS rough_conversion,
            IFF(
                POSITION(
                    '.000000000000000000' IN rough_conversion
                ) > 0,
                LEFT(rough_conversion, LENGTH(rough_conversion) - 19),
                REGEXP_REPLACE(
                    rough_conversion,
                    '0*$',
                    ''
                )
            ) AS eth_value_precise
        FROM
            {{ ref('silver__transactions') }}
            LEFT JOIN {{ ref('silver__state_hashes') }} USING (block_number)
            LEFT JOIN {{ ref('silver__submission_hashes') }} USING (block_number)
    )
