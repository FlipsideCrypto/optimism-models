{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_get_traces(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'debug_traceBlockByNumber', 'sql_limit', {{var('sql_limit','150000')}}, 'producer_batch_size', {{var('producer_batch_size','75000')}}, 'worker_batch_size', {{var('worker_batch_size','75000')}}, 'batch_call_limit', {{var('batch_call_limit','1')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH blocks AS (

    SELECT
        block_number
    FROM
        {{ ref("streamline__blocks") }}
    WHERE
        block_number <= 22207817
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_debug_traceBlockByNumber") }}
    WHERE
        block_number <= 22207817
)
SELECT
    PARSE_JSON(
        CONCAT(
            '{"jsonrpc": "2.0",',
            '"method": "arbtrace_block", "params":["',
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
            '",{"tracer": "callTracer"}',
            '],"id":"',
            block_number :: STRING,
            '"}'
        )
    ) AS request
FROM
    blocks
ORDER BY
    block_number ASC
