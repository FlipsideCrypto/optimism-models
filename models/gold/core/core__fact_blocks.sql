{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    A.block_number AS block_number,
    HASH AS block_hash, --new column
    block_timestamp,
    'mainnet' AS network,
    tx_count,
    SIZE,
    miner, --new column
    extra_data,
    parent_hash,
    gas_used,
    gas_limit,
    difficulty,
    total_difficulty,
    sha3_uncles,
    uncles AS uncle_blocks,
    nonce, --new column
    receipts_root,
    state_root, --new column
    transactions_root, --new column
    logs_bloom, --new column
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
        ), --deprecate
        COALESCE(
            C.inserted_timestamp,
            '2000-01-01'
        ), --deprecate
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
        ), --deprecate
        COALESCE(
            C.modified_timestamp,
            '2000-01-01'
        ), --deprecate
        COALESCE(
            d.modified_timestamp,
            '2000-01-01'
        )
    ) AS modified_timestamp,
    'optimism' AS blockchain, --deprecate
    HASH, --deprecate
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
    ) AS block_header_json, --deprecate
    state_tx_hash AS l1_state_root_tx_hash, --deprecate, may be separate table
    state_batch_index AS l1_state_root_batch_index, --deprecate, may be separate table
    l1_submission_tx_hash, --deprecate, may be separate table
    l1_submission_batch_index, --deprecate, may be separate table
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
    ) AS l1_submission_details, --deprecate, may be separate table
    withdrawals_root --deprecate, may be separate table
FROM
    {{ ref('silver__blocks') }} A
    LEFT JOIN {{ ref('silver__state_hashes') }}
    b --deprecate
    ON A.block_number = b.block_number
    LEFT JOIN {{ ref('silver__submission_hashes') }} C --deprecate
    ON A.block_number = C.block_number
    LEFT JOIN {{ ref('silver__tx_count') }}
    d
    ON A.block_number = d.block_number
