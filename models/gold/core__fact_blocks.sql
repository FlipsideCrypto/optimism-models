{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    network,
    blockchain,
    tx_count,
    difficulty,
    total_difficulty,
    extra_data,
    gas_limit,
    gas_used,
    HASH,
    parent_hash,
    receipts_root,
    sha3_uncles,
    SIZE,
    uncle_blocks,
    block_header_json,
    state_tx_hash AS l1_state_root_tx_hash,
    state_batch_index AS l1_state_root_batch_index,
    l1_submission_tx_hash,
    l1_submission_batch_index AS l1_submission_batch_index
FROM
    {{ ref('silver__blocks') }}
    LEFT JOIN {{ ref('bronze__state_hashes') }}
    ON block_number BETWEEN state_min_block
    AND state_max_block
    LEFT JOIN {{ ref('bronze__submission_hashes') }}
    ON block_number BETWEEN sub_min_block
    AND sub_max_block
