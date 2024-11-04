{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'pool_address',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated']
) }}

WITH pool_creation AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3), '.{64}') AS segmented,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS token0,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token1,
        IFF(utils.udf_hex_to_int(segmented [0] :: STRING) = 0, FALSE, TRUE) AS stable,
        '0x' || SUBSTR(
            segmented [1] :: STRING,
            25,
            40
        ) AS pool_address,
        utils.udf_hex_to_int(
            segmented [2] :: STRING
        ) AS pool_id,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2022-06-01'
        AND topics [0] :: STRING = '0xc4805696c66d7cf352fc1d6bb633ad5ee82f6cb577c453024b6e0eb8306c6fc9' -- pair created
        AND contract_address = '0x25cbddb98b35ab1ff77413456b31ec81a6b6b746' --velo pair factory
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY pool_address
ORDER BY
    _inserted_timestamp DESC)) = 1
),
build_request AS (
    SELECT
        pool_address,
        block_number,
        '0x06fdde03' AS function_sig,
        -- name
        utils.udf_json_rpc_call(
            'eth_call',
            [{'to': pool_address, 'from': null, 'data': function_sig}, utils.udf_int_to_hex(block_number)],
            concat_ws(
                '-',
                contract_address,
                function_sig,
                block_number
            )
        ) AS rpc_request
    FROM
        pool_creation
),
requests AS (
    SELECT
        *,
        live.udf_api(
            'POST',
            CONCAT(
                '{service}',
                '/',
                '{Authentication}'
            ),{},
            rpc_request,
            'Vault/prod/optimism/quicknode/mainnet'
        ) AS read_output,
        read_output :data :result AS results
    FROM
        build_request
),
results_decoded AS (
    SELECT
        pool_address,
        regexp_substr_all(SUBSTR(results, 3), '.{64}') AS segmented_data,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) / 32 AS offset,
        utils.udf_hex_to_int(
            segmented_data [offset] :: STRING
        ) * 2 AS string_length,
        utils.udf_hex_to_string(
            SUBSTR(
                segmented_data [offset + 1 ] :: STRING,
                1,
                string_length
            )
        ) AS pool_name
    FROM
        requests
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    token0,
    token1,
    stable,
    pool_address,
    pool_name,
    pool_id,
    _log_id,
    _inserted_timestamp
FROM
    pool_creation
    LEFT JOIN results_decoded USING (pool_address)
