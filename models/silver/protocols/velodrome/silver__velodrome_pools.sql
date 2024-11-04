{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'created_block',
    full_refresh = false,
    tags = ['stale']
) }}

WITH pool_creation AS (

    SELECT
        block_timestamp AS created_timestamp,
        block_number,
        tx_hash AS created_hash,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS token0_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token1_address,
        CONCAT('0x', SUBSTR(DATA :: STRING, 91, 40)) AS pool_address,
        CASE
            WHEN SUBSTR(
                DATA,
                66,
                1
            ) = 1 THEN 'stable'
            ELSE 'volatile'
        END AS pool_type,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0xc4805696c66d7cf352fc1d6bb633ad5ee82f6cb577c453024b6e0eb8306c6fc9' -- pair created
        AND contract_address = '0x25cbddb98b35ab1ff77413456b31ec81a6b6b746' --velo deployer

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
function_sigs AS (
    SELECT
        '0x06fdde03' AS function_sig,
        'name' AS function_name
    UNION ALL
    SELECT
        '0x95d89b41' AS function_sig,
        'symbol' AS function_name
    UNION ALL
    SELECT
        '0x313ce567' AS function_sig,
        'decimals' AS function_name
),
all_inputs AS (
    SELECT
        pool_address AS contract_address,
        block_number,
        function_sig,
        'pool' AS address_label
    FROM
        pool_creation
        JOIN function_sigs
        ON 1 = 1
    UNION ALL
    SELECT
        token0_address AS contract_address,
        block_number,
        function_sig,
        'token0' AS address_label
    FROM
        pool_creation
        JOIN function_sigs
        ON 1 = 1
    UNION ALL
    SELECT
        token1_address AS contract_address,
        block_number,
        function_sig,
        'token1' AS address_label
    FROM
        pool_creation
        JOIN function_sigs
        ON 1 = 1
),
build_rpc_requests AS (
    SELECT
        contract_address,
        block_number,
        function_sig,
        RPAD(
            function_sig,
            64,
            '0'
        ) AS input,
        utils.udf_json_rpc_call(
            'eth_call',
            [{'to': contract_address, 'from': null, 'data': input}, utils.udf_int_to_hex(block_number)],
            concat_ws(
                '-',
                contract_address,
                input,
                block_number
            )
        ) AS rpc_request,
        ROW_NUMBER() over (
            ORDER BY
                block_number
        ) AS row_no,
        CEIL(
            row_no / 300
        ) AS batch_no
    FROM
        all_inputs
),
all_reads AS (

{% if is_incremental() %}
{% for item in range(3) %}
    (
    SELECT
        live.udf_api('POST', CONCAT('{service}', '/', '{Authentication}'),{}, batch_rpc_request, 'Vault/prod/optimism/quicknode/mainnet') AS read_output, SYSDATE() AS _inserted_timestamp
    FROM
        (
    SELECT
        ARRAY_AGG(rpc_request) batch_rpc_request
    FROM
        build_rpc_requests
    WHERE
        batch_no = {{ item }} + 1
        AND batch_no IN (
    SELECT
        DISTINCT batch_no
    FROM
        build_rpc_requests))) {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
{% else %}
    {% for item in range(20) %}
        (
    SELECT
        live.udf_api('POST', CONCAT('{service}', '/', '{Authentication}'),{}, batch_rpc_request, 'Vault/prod/optimism/quicknode/mainnet') AS read_output, SYSDATE() AS _inserted_timestamp
    FROM
        (
    SELECT
        ARRAY_AGG(rpc_request) batch_rpc_request
    FROM
        build_rpc_requests
    WHERE
        batch_no = {{ item }} + 1
        AND batch_no IN (
    SELECT
        DISTINCT batch_no
    FROM
        build_rpc_requests))) {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
{% endif %}),
reads_adjusted AS (
    SELECT
        VALUE :id :: STRING AS read_id,
        VALUE :result :: STRING AS read_result,
        SPLIT(
            read_id,
            '-'
        ) AS read_id_object,
        read_id_object [0] :: STRING AS contract_address,
        read_id_object [2] :: STRING AS block_number,
        LEFT(
            read_id_object [1] :: STRING,
            10
        ) AS function_sig,
        _inserted_timestamp
    FROM
        all_reads,
        LATERAL FLATTEN (
            input => read_output :data
        )
),
details AS (
    SELECT
        contract_address,
        function_sig,
        function_name,
        read_result,
        regexp_substr_all(SUBSTR(read_result, 3, len(read_result)), '.{64}') AS segmented_output,
        _inserted_timestamp
    FROM
        reads_adjusted
        LEFT JOIN function_sigs USING(function_sig)
),
pools AS (
    SELECT
        d.contract_address AS pool_address,
        MIN(
            CASE
                WHEN function_name = 'symbol' THEN utils.udf_hex_to_string(
                    segmented_output [2] :: STRING
                )
            END
        ) AS pool_symbol,
        MIN(
            CASE
                WHEN function_name = 'name' THEN utils.udf_hex_to_string(
                    segmented_output [2] :: STRING
                )
            END
        ) AS pool_name,
        MIN(
            CASE
                WHEN read_result :: STRING = '0x' THEN NULL
                ELSE utils.udf_hex_to_int(
                    read_result :: STRING
                )
            END
        ) :: INTEGER AS pool_decimals,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        details d
        LEFT JOIN all_inputs i
        ON d.contract_address = i.contract_address
    WHERE
        address_label = 'pool'
    GROUP BY
        1
),
token0 AS (
    SELECT
        d.contract_address AS token0_address,
        MIN(
            CASE
                WHEN function_name = 'symbol' THEN utils.udf_hex_to_string(
                    segmented_output [2] :: STRING
                )
            END
        ) AS token0_symbol,
        MIN(
            CASE
                WHEN function_name = 'name' THEN utils.udf_hex_to_string(
                    segmented_output [2] :: STRING
                )
            END
        ) AS token0_name,
        MIN(
            CASE
                WHEN function_name = 'decimals'
                AND read_result :: STRING <> '0x' THEN utils.udf_hex_to_int(
                    segmented_output [0] :: STRING
                )
                ELSE NULL
            END
        ) :: INTEGER AS token0_decimals,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        details d
        LEFT JOIN all_inputs i
        ON d.contract_address = i.contract_address
    WHERE
        address_label = 'token0'
    GROUP BY
        1
),
token1 AS (
    SELECT
        d.contract_address AS token1_address,
        MIN(
            CASE
                WHEN function_name = 'symbol' THEN utils.udf_hex_to_string(
                    segmented_output [2] :: STRING
                )
            END
        ) AS token1_symbol,
        MIN(
            CASE
                WHEN function_name = 'name' THEN utils.udf_hex_to_string(
                    segmented_output [2] :: STRING
                )
            END
        ) AS token1_name,
        MIN(
            CASE
                WHEN function_name = 'decimals'
                AND read_result :: STRING <> '0x' THEN utils.udf_hex_to_int(
                    segmented_output [0] :: STRING
                )
                ELSE NULL
            END
        ) :: INTEGER AS token1_decimals,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        details d
        LEFT JOIN all_inputs i
        ON d.contract_address = i.contract_address
    WHERE
        address_label = 'token1'
    GROUP BY
        1
)
SELECT
    pool_address,
    pool_name,
    pool_type,
    REGEXP_REPLACE(
        pool_symbol,
        '[^a-zA-Z0-9/-]+'
    ) AS pool_symbol,
    pool_decimals,
    REGEXP_REPLACE(
        token0_symbol,
        '[^a-zA-Z0-9]+'
    ) AS token0_symbol,
    REGEXP_REPLACE(
        token1_symbol,
        '[^a-zA-Z0-9]+'
    ) AS token1_symbol,
    C.token0_address,
    C.token1_address,
    token0_decimals,
    token1_decimals,
    created_timestamp,
    block_number AS created_block,
    created_hash,
    A._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['pool_address']
    ) }} AS pools_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pools A
    LEFT JOIN pool_creation C USING(pool_address)
    LEFT JOIN token0
    ON C.token0_address = token0.token0_address
    LEFT JOIN token1
    ON C.token1_address = token1.token1_address
