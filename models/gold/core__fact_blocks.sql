{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    A.block_number AS block_number,
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
    {{ ref('silver__blocks') }} A
    LEFT JOIN {{ ref('silver__state_hashes') }}
    b
    ON A.block_number = b.block_number
    LEFT JOIN {{ ref('silver__submission_hashes') }} C
    ON A.block_number = C.block_number
