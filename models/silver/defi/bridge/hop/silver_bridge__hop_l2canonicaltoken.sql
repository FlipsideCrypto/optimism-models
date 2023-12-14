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
inputs AS (
    SELECT
        amm_wrapper_address,
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
        amm_wrapper_address,
        block_number,
        function_sig,
        function_name,
        function_input,
        DATA,
        utils.udf_json_rpc_call(
            'eth_call',
            [{ 'to': amm_wrapper_address, 'from': null, 'data': data }, utils.udf_int_to_hex(block_number) ]
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
        amm_wrapper_address,
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
    amm_wrapper_address,
    CASE
        WHEN contract_address = '0x03d7f750777ec48d39d080b020d83eb2cb4e3547' THEN '0xc5102fe9359fd9a28f877a67e36b0f050d81a3cc'
        ELSE CONCAT('0x', SUBSTR(read_result, 27, 40))
    END AS token_address,
    _inserted_timestamp
FROM
    reads_flat
    LEFT JOIN base_contracts USING(amm_wrapper_address)
WHERE
    token_address <> '0x'
    AND token_address IS NOT NULL
