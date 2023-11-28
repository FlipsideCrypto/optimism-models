{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    state_tx_hash AS l1_state_root_tx_hash,
    state_block_number AS l1_block_number,
    state_block_timestamp AS l1_block_timestamp,
    COALESCE(
        state_batch_index,
        bedrock_state_batch_index
    ) AS l1_state_root_batch_index,
    COALESCE(
        state_batch_root,
        bedrock_state_batch_root
    ) AS batch_root,
    state_batch_size AS batch_size,
    state_prev_total_elements AS prev_total_elements,
    state_min_block AS op_min_block,
    state_max_block AS op_max_block,
    COALESCE(
        state_hashes_id,
        {{ dbt_utils.generate_surrogate_key(
            ['state_block_number']
        ) }}
    ) AS fact_l1_state_root_submissions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('bronze__state_hashes') }}
