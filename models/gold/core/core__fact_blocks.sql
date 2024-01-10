{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    A.block_number AS block_number,
    block_timestamp,
    'mainnet' AS network,
    'optimism' AS blockchain,
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
    uncles AS uncle_blocks,
    OBJECT_CONSTRUCT(
        'baseFeePerGas',
        base_fee_per_gas,
        'difficulty',
        difficulty,
        'extraData',
        extra_data,
        'gasLimit',
        gas_limit,
        'gasUsed',
        gas_used,
        'hash',
        HASH,
        'logsBloom',
        logs_bloom,
        'miner',
        miner,
        'nonce',
        nonce,
        'number',
        NUMBER,
        'parentHash',
        parent_hash,
        'receiptsRoot',
        receipts_root,
        'sha3Uncles',
        sha3_uncles,
        'size',
        SIZE,
        'stateRoot',
        state_root,
        'timestamp',
        block_timestamp,
        'totalDifficulty',
        total_difficulty,
        'transactionsRoot',
        transactions_root,
        'uncles',
        uncles
    ) AS block_header_json,
    state_tx_hash AS l1_state_root_tx_hash,
    state_batch_index AS l1_state_root_batch_index,
    l1_submission_tx_hash,
    l1_submission_batch_index,
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
    COALESCE (
        blocks_id,
        {{ dbt_utils.generate_surrogate_key(
            ['a.block_number']
        ) }}
    ) AS fact_blocks_id,
    GREATEST(
        COALESCE(
            A.inserted_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            b.inserted_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            C.inserted_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            d.inserted_timestamp,
            '2000-01-01'
        )
    ) AS inserted_timestamp,
    GREATEST(
        COALESCE(
            A.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            b.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            C.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            d.modified_timestamp,
            '2000-01-01'
        )
    ) AS modified_timestamp,
    withdrawals,
    withdrawals_root
FROM
    {{ ref('silver__blocks') }} A
    LEFT JOIN {{ ref('silver__state_hashes') }}
    b
    ON A.block_number = b.block_number
    LEFT JOIN {{ ref('silver__submission_hashes') }} C
    ON A.block_number = C.block_number
    LEFT JOIN {{ ref('silver__tx_count') }}
    d
    ON A.block_number = d.block_number
