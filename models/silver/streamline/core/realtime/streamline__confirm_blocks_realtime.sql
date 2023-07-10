{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'confirm_blocks', 'sql_limit', {{var('sql_limit','40000')}}, 'producer_batch_size', {{var('producer_batch_size','10000')}}, 'worker_batch_size', {{var('worker_batch_size','1000')}}, 'batch_call_limit', {{var('batch_call_limit','10')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH look_back AS (

    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_hour") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) = 6
),
tbl AS (
    SELECT
        block_number
    FROM
        {{ ref("streamline__blocks") }}
    WHERE
        block_number IS NOT NULL
        AND block_number <= (
            SELECT
                block_number
            FROM
                look_back
        )
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_confirmed_blocks") }}
    WHERE
        block_number IS NOT NULL
        AND block_number <= (
            SELECT
                block_number
            FROM
                look_back
        )
)
SELECT
    PARSE_JSON(
        CONCAT(
            '{"jsonrpc": "2.0",',
            '"method": "eth_getBlockByNumber", "params":["',
            REPLACE(
                concat_ws(
                    '',
                    '0x',
                    to_char(
                        block_number :: INTEGER,
                        'XXXXXXXX'
                    )
                ),
                ' ',
                ''
            ),
            '", false],"id":"',
            block_number :: INTEGER,
            '"}'
        )
    ) AS request
FROM
    tbl
ORDER BY
    block_number ASC
