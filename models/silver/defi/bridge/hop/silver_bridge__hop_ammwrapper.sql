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
inputs AS (
    SELECT
        contract_address,
        block_number,
        function_sig,
        function_name,
        0 AS function_input,
        CONCAT(
            function_sig,
            LPAD(
                function_input,
                64,
                0
            )
        ) AS DATA
    FROM
        base_contracts
        JOIN function_sigs
        ON 1 = 1
),
contract_reads AS (
    SELECT
        contract_address,
        block_number,
        function_sig,
        function_name,
        function_input,
        DATA,
        utils.udf_json_rpc_call(
            'eth_call',
            [{ 'to': contract_address, 'from': null, 'data': data }, utils.udf_int_to_hex(block_number) ]
        ) AS rpc_request,
        live.udf_api(
            node_url,
            rpc_request
        ) AS read_output,
        SYSDATE() AS _inserted_timestamp
    FROM
        inputs
        JOIN {{ source(
            'streamline_crosschain',
            'node_mapping'
        ) }}
        ON 1 = 1
        AND chain = 'optimism'
),
reads_flat AS (
    SELECT
        read_output,
        read_output :data :id :: STRING AS read_id,
        read_output :data :result :: STRING AS read_result,
        SPLIT(
            read_id,
            '-'
        ) AS read_id_object,
        function_sig,
        function_name,
        function_input,
        DATA,
        contract_address,
        block_number,
        _inserted_timestamp
    FROM
        contract_reads
)
SELECT
    read_output,
    read_id,
    read_result,
    read_id_object,
    function_sig,
    function_name,
    function_input,
    DATA,
    block_number,
    contract_address,
    CONCAT('0x', SUBSTR(read_result, 27, 40)) AS amm_wrapper_address,
    _inserted_timestamp
FROM
    reads_flat
