{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    tags = ['curated']
) }}

WITH base_contracts AS (

    SELECT
        contract_address,
        amm_wrapper_address,
        block_number
    FROM
        {{ ref('silver_bridge__hop_ammwrapper') }}

{% if is_incremental() %}
WHERE
    amm_wrapper_address NOT IN (
        SELECT
            DISTINCT amm_wrapper_address
        FROM
            {{ this }}
    )
{% endif %}
),
function_sigs AS (
    SELECT
        '0x1ee1bf67' AS function_sig,
        'l2CanonicalToken' AS function_name
),
inputs_contracts AS (
    SELECT
        amm_wrapper_address,
        block_number,
        function_sig,
        (ROW_NUMBER() over (PARTITION BY amm_wrapper_address
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
                        amm_wrapper_address,
                        block_number,
                        function_sig,
                        function_input,
                        CONCAT(
                            '[\'',
                            amm_wrapper_address,
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
        read_id_object [0] :: STRING AS amm_wrapper_address,
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
    amm_wrapper_address,
    block_number,
    function_sig,
    function_name,
    CASE
        WHEN contract_address = '0x03d7f750777ec48d39d080b020d83eb2cb4e3547' THEN '0xc5102fe9359fd9a28f877a67e36b0f050d81a3cc'
        ELSE CONCAT('0x', SUBSTR(read_result, 27, 40))
    END AS token_address,
    _inserted_timestamp
FROM
    reads_flat
    LEFT JOIN function_sigs USING(function_sig)
    LEFT JOIN base_contracts USING(amm_wrapper_address)
WHERE
    token_address <> '0x'
    AND token_address IS NOT NULL
