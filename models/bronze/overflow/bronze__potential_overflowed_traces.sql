{{ config (
    materialized = "view",
    tags = ['overflowed_traces']
) }}

WITH impacted_blocks AS (

    SELECT
        blocks_impacted_array
    FROM
        {{ ref("silver_observability__traces_completeness") }}
    ORDER BY
        test_timestamp DESC
    LIMIT
        1
), all_missing AS (
    SELECT
        DISTINCT VALUE :: INT AS block_number
    FROM
        impacted_blocks,
        LATERAL FLATTEN (
            input => blocks_impacted_array
        )
),
all_txs AS (
    SELECT
        block_number,
        POSITION AS tx_position,
        tx_hash
    FROM
        {{ ref("silver__transactions") }}
        JOIN all_missing USING (block_number)
),
missing_txs AS (
    SELECT
        DISTINCT txs.block_number,
        txs.tx_position,
        file_name
    FROM
        all_txs txs
        LEFT JOIN {{ source(
            "optimism_gold",
            "fact_traces"
        ) }}
        tr2 USING (
            block_number,
            tx_position
        )
        JOIN {{ ref("streamline__traces_complete") }} USING (block_number)
        LEFT JOIN {{ source(
            'optimism_silver',
            'overflowed_traces'
        ) }}
        ot USING (
            block_number,
            tx_position
        )
    WHERE
        tr2.block_number IS NULL
        AND ot.block_number IS NULL
)
SELECT
    block_number,
    tx_position AS POSITION,
    file_name,
    build_scoped_file_url(
        @streamline.bronze.external_tables,
        file_name
    ) AS file_url,
    ['block_number', 'array_index'] AS index_cols,
    ROW_NUMBER() over (
        ORDER BY
            block_number ASC,
            POSITION ASC
    ) AS row_no
FROM
    missing_txs
ORDER BY
    block_number ASC,
    POSITION ASC
