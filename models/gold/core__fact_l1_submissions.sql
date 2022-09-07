{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    l1_submission_tx_hash,
    l1_submission_block_number AS l1_block_number,
    l1_submission_block_timestamp AS l1_block_timestamp,
    l1_submission_batch_index AS l1_submission_batch_index,
    l1_submission_batch_root AS batch_root,
    l1_submission_size AS batch_size,
    l1_submission_prev_total_elements AS prev_total_elements,
    sub_min_block AS op_min_block,
    sub_max_block AS op_max_block
FROM
    {{ ref('bronze__submission_hashes') }}
