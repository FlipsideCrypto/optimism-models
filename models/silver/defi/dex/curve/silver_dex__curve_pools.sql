{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'pool_address',
    full_refresh = false,
    tags = ['curated']
) }}

WITH contract_deployments AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address AS deployer_address,
        to_address AS contract_address,
        concat_ws(
            '-',
            block_number,
            tx_position,
            CONCAT(
                type,
                '_',
                trace_address
            )
        ) AS _call_id,
        modified_timestamp AS _inserted_timestamp,
        ROW_NUMBER() over (
            ORDER BY
                contract_address
        ) AS row_num
    FROM
        {{ ref(
            'core__fact_traces'
        ) }}
    WHERE
        -- curve contract deployers
        from_address IN (
            '0x2db0e83599a91b508ac268a6197b8b14f5e72840',
            '0x7eeac6cddbd1d0b8af061742d41877d7f707289a',
            '0x745748bcfd8f9c2de519a71d789be8a63dd7d66c'
        )
        AND TYPE ILIKE 'create%'
        AND tx_succeeded
        AND trace_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY to_address
ORDER BY
    block_timestamp ASC)) = 1
),
function_sigs AS (
    SELECT
        '0x87cb4f57' AS function_sig,
        'base_coins' AS function_name
    UNION ALL
    SELECT
        '0xb9947eb0' AS function_sig,
        'underlying_coins' AS function_name
    UNION ALL
    SELECT
        '0xc6610657' AS function_sig,
        'coins' AS function_name
    UNION ALL
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
function_inputs AS (
    SELECT
        SEQ4() AS function_input
    FROM
        TABLE(GENERATOR(rowcount => 8))
),
inputs_coins AS (
    SELECT
        deployer_address,
        contract_address,
        block_number,
        function_sig,
        (ROW_NUMBER() over (PARTITION BY contract_address
    ORDER BY
        block_number)) - 1 AS function_input
    FROM
        contract_deployments
        JOIN function_sigs
        ON 1 = 1
        JOIN function_inputs
        ON 1 = 1
    WHERE
        function_name = 'coins'
),
inputs_base_coins AS (
    SELECT
        deployer_address,
        contract_address,
        block_number,
        function_sig,
        (ROW_NUMBER() over (PARTITION BY contract_address
    ORDER BY
        block_number)) - 1 AS function_input
    FROM
        contract_deployments
        JOIN function_sigs
        ON 1 = 1
        JOIN function_inputs
        ON 1 = 1
    WHERE
        function_name = 'base_coins'
),
inputs_underlying_coins AS (
    SELECT
        deployer_address,
        contract_address,
        block_number,
        function_sig,
        (ROW_NUMBER() over (PARTITION BY contract_address
    ORDER BY
        block_number)) - 1 AS function_input
    FROM
        contract_deployments
        JOIN function_sigs
        ON 1 = 1
        JOIN function_inputs
        ON 1 = 1
    WHERE
        function_name = 'underlying_coins'
),
inputs_pool_details AS (
    SELECT
        deployer_address,
        contract_address,
        block_number,
        function_sig,
        NULL AS function_input
    FROM
        contract_deployments
        JOIN function_sigs
        ON 1 = 1
    WHERE
        function_name IN (
            'name',
            'symbol',
            'decimals'
        )
),
all_inputs AS (
    SELECT
        deployer_address,
        contract_address,
        block_number,
        function_sig,
        function_input
    FROM
        inputs_coins
    UNION ALL
    SELECT
        deployer_address,
        contract_address,
        block_number,
        function_sig,
        function_input
    FROM
        inputs_base_coins
    UNION ALL
    SELECT
        deployer_address,
        contract_address,
        block_number,
        function_sig,
        function_input
    FROM
        inputs_underlying_coins
    UNION ALL
    SELECT
        deployer_address,
        contract_address,
        block_number,
        function_sig,
        function_input
    FROM
        inputs_pool_details
),
build_rpc_requests AS (
    SELECT
        deployer_address,
        contract_address,
        block_number,
        function_sig,
        function_input,
        CONCAT(
            function_sig,
            LPAD(IFNULL(function_input, 0), 64, '0')
        ) AS input,
        utils.udf_json_rpc_call(
            'eth_call',
            [{'to': contract_address, 'from': null, 'data':input }, utils.udf_int_to_hex(block_number)],
            concat_ws(
                '-',
                contract_address,
                input,
                block_number
            )
        ) AS rpc_request,
        row_num,
        CEIL(
            row_num / 50
        ) AS batch_no
    FROM
        all_inputs
        LEFT JOIN contract_deployments USING(contract_address)
),
pool_token_reads AS (

{% if is_incremental() %}
{% for item in range(6) %}
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
    {% for item in range(60) %}
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
        RIGHT(
            read_id_object [1],
            LENGTH(
                read_id_object [1] - 10
            )
        ) :: INT AS function_input,
        _inserted_timestamp
    FROM
        pool_token_reads,
        LATERAL FLATTEN(
            input => read_output :data
        )
),
tokens AS (
    SELECT
        contract_address,
        function_sig,
        function_name,
        function_input,
        read_result,
        regexp_substr_all(SUBSTR(read_result, 3, len(read_result)), '.{64}') [0] AS segmented_token_address,
        _inserted_timestamp
    FROM
        reads_adjusted
        LEFT JOIN function_sigs USING(function_sig)
    WHERE
        function_name IN (
            'coins',
            'base_coins',
            'underlying_coins'
        )
        AND read_result IS NOT NULL
),
pool_details AS (
    SELECT
        contract_address,
        function_sig,
        function_name,
        function_input,
        read_result,
        regexp_substr_all(SUBSTR(read_result, 3, len(read_result)), '.{64}') AS segmented_output,
        _inserted_timestamp
    FROM
        reads_adjusted
        LEFT JOIN function_sigs USING(function_sig)
    WHERE
        function_name IN (
            'name',
            'symbol',
            'decimals'
        )
        AND read_result IS NOT NULL
),
all_pools AS (
    SELECT
        t.contract_address AS pool_address,
        CONCAT('0x', SUBSTRING(t.segmented_token_address, 25, 40)) AS token_address,
        function_input AS token_id,
        function_name AS token_type,
        MIN(
            CASE
                WHEN p.function_name = 'symbol' THEN utils.udf_hex_to_string(RTRIM(p.segmented_output [2] :: STRING, 0))
            END
        ) AS pool_symbol,
        MIN(
            CASE
                WHEN p.function_name = 'name' THEN CONCAT(
                    utils.udf_hex_to_string(
                        p.segmented_output [2] :: STRING
                    ),
                    utils.udf_hex_to_string(
                        segmented_output [3] :: STRING
                    )
                )
            END
        ) AS pool_name,
        MIN(
            CASE
                WHEN p.read_result :: STRING = '0x' THEN NULL
                ELSE utils.udf_hex_to_int(LEFT(p.read_result :: STRING, 66))
            END
        ) :: INTEGER AS pool_decimals,
        CONCAT(
            t.contract_address,
            '-',
            CONCAT('0x', SUBSTRING(t.segmented_token_address, 25, 40)),
            '-',
            function_input,
            '-',
            function_name
        ) AS pool_id,
        MAX(
            t._inserted_timestamp
        ) AS _inserted_timestamp
    FROM
        tokens t
        LEFT JOIN pool_details p USING(contract_address)
    WHERE
        token_address IS NOT NULL
        AND token_address <> '0x0000000000000000000000000000000000000000'
    GROUP BY
        1,
        2,
        3,
        4
),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        deployer_address,
        pool_address,
        CASE
            WHEN token_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0x4200000000000000000000000000000000000006'
            ELSE token_address
        END AS token_address,
        token_id,
        token_type,
        CASE
            WHEN token_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 'WETH'
            WHEN pool_symbol IS NULL THEN C.token_symbol
            ELSE pool_symbol
        END AS pool_symbol,
        pool_name,
        CASE
            WHEN token_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '18'
            WHEN pool_decimals IS NULL THEN C.token_decimals
            ELSE pool_decimals
        END AS pool_decimals,
        pool_id,
        _call_id,
        A._inserted_timestamp
    FROM
        all_pools A
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON A.token_address = C.contract_address
        LEFT JOIN contract_deployments d
        ON A.pool_address = d.contract_address qualify(ROW_NUMBER() over(PARTITION BY pool_address, token_address
    ORDER BY
        A._inserted_timestamp DESC)) = 1
)
SELECT
    *,
    ROW_NUMBER() over (
        PARTITION BY pool_address
        ORDER BY
            token_address ASC
    ) AS token_num
FROM
    FINAL
