{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "pool_address",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH created_pools AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        LOWER(CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))) AS token0_address,
        LOWER(CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))) AS token1_address,
        utils.udf_hex_to_int(
            's2c',
            topics [3] :: STRING
        ) :: INTEGER AS tick_spacing,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS pool_address,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] = '0xab0d57f0df537bb25e80245ef7748fa62353808c54d6e528a9dd20887aed9ac2'
        AND contract_address = LOWER('0xCc0bDDB707055e04e497aB22a59c2aF4391cd12F')
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
initial_info AS (
    SELECT
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int('s2c', CONCAT('0x', segmented_data [0] :: STRING)) :: FLOAT AS init_sqrtPriceX96,
        utils.udf_hex_to_int('s2c', CONCAT('0x', segmented_data [1] :: STRING)) :: FLOAT AS init_tick,
        pow(
            1.0001,
            init_tick
        ) AS init_price_1_0_unadj,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0x98636036cb66a9c19a37435efc1e90142190214e8abeb821bdba3f2990dd4c95'
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    p.contract_address,
    token0_address,
    token1_address,
    tick_spacing,
    pool_address,
    COALESCE(
        init_tick,
        0
    ) AS init_tick,
    p._log_id AS _id,
    p._inserted_timestamp
FROM
    created_pools p
    LEFT JOIN initial_info i
    ON p.pool_address = i.contract_address qualify(ROW_NUMBER() over(PARTITION BY p.pool_address
ORDER BY
    p._inserted_timestamp DESC)) = 1
