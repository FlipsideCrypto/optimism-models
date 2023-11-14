{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    tags = ['curated']
) }}

WITH base_contracts AS (

    SELECT
        contract_address,
        MAX(block_number) AS block_number
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xe35dddd4ea75d7e9b3fe93af4f4e40e778c3da4074c9d93e7c6536f1e803c1eb'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND contract_address NOT IN (
    SELECT
        DISTINCT contract_address
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    1
),
function_sigs AS (
    SELECT
        '0xe9cdfe51' AS function_sig,
        'ammWrapper' AS function_name
),
inputs_contracts AS (
    SELECT
        contract_address,
        block_number,
        function_sig,
        (ROW_NUMBER() over (PARTITION BY contract_address
    ORDER BY
        block_number)) - 1 AS function_input
    FROM
        base_contracts
        JOIN function_sigs
        ON 1 = 1
),
contract_reads AS (
    SELECT
        ethereum.streamline.udf_json_rpc_read_calls(
            node_url,
            headers,
            PARSE_JSON(batch_read)
        ) AS read_output,
        SYSDATE() AS _inserted_timestamp
    FROM
        (
            SELECT
                CONCAT('[', LISTAGG(read_input, ','), ']') AS batch_read
            FROM
                (
                    SELECT
                        contract_address,
                        block_number,
                        function_sig,
                        function_input,
                        CONCAT(
                            '[\'',
                            contract_address,
                            '\',',
                            block_number,
                            ',\'',
                            function_sig,
                            '\',\'',
                            function_input,
                            '\']'
                        ) AS read_input
                    FROM
                        inputs_contracts
                ) ready_reads_contracts
        ) batch_reads_contracts
        JOIN {{ source(
            'streamline_crosschain',
            'node_mapping'
        ) }}
        ON 1 = 1
        AND chain = 'optimism'
),
reads_flat AS (
    SELECT
        VALUE :id :: STRING AS read_id,
        VALUE :result :: STRING AS read_result,
        SPLIT(
            read_id,
            '-'
        ) AS read_id_object,
        read_id_object [0] :: STRING AS contract_address,
        read_id_object [1] :: STRING AS block_number,
        read_id_object [2] :: STRING AS function_sig,
        read_id_object [3] :: STRING AS function_input,
        _inserted_timestamp
    FROM
        contract_reads,
        LATERAL FLATTEN(
            input => read_output [0] :data
        )
)
SELECT
    contract_address,
    block_number,
    function_sig,
    function_name,
    CONCAT('0x', SUBSTR(read_result, 27, 40)) AS amm_wrapper_address,
    _inserted_timestamp
FROM
    reads_flat
    LEFT JOIN function_sigs USING(function_sig)
{# WHERE
    token_address <> '0x'
    AND token_address IS NOT NULL #}
