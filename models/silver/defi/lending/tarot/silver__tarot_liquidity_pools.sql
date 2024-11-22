{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH factory_pull AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS vtarot,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token0,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS token1,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS ctarot,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS borrowable1,
        CONCAT('0x', SUBSTR(segmented_data [2] :: STRING, 25, 40)) AS borrowable2,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INTEGER AS lendingpoolId,
        l._inserted_timestamp,
        l._log_id
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        contract_address = LOWER('0xD7cABeF2c1fD77a31c5ba97C724B82d3e25fC83C')
        AND topics [0] = '0x4c3ab495dc8ebd1b2f3232d7632e54411bc7e4d111475e7fbbd5547d9a28c495'

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
lp_token_pull AS (
    SELECT
        t.tx_hash,
        C.*
    FROM
        {{ ref('silver__traces') }}
        t
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON to_address = contract_address
    WHERE
        block_timestamp >= (
            SELECT
                MIN(block_timestamp)
            FROM
                factory_pull
        )
        AND t.tx_hash IN (
            SELECT
                tx_hash
            FROM
                factory_pull
        )
        AND identifier = 'STATICCALL_3_1_0'
)
SELECT
    l.tx_hash,
    block_number,
    block_timestamp,
    lp.contract_address AS lp_token_address,
    lp.token_name AS lp_token_name,
    lp.token_symbol AS lp_token_symbol,
    lp.token_decimals AS lp_token_decimals,
    vtarot,
    token0,
    c1.token_name AS token0_name,
    c1.token_symbol AS token0_symbol,
    c1.token_decimals AS token0_decimals,
    token1,
    c2.token_name AS token1_name,
    c2.token_symbol AS token1_symbol,
    c2.token_decimals AS token1_decimals,
    ctarot,
    borrowable1,
    borrowable2,
    utils.udf_hex_to_int(
        segmented_data [3] :: STRING
    ) :: INTEGER AS lendingpoolId,
    l._inserted_timestamp,
    l._log_id
FROM
    factory_pull l
    LEFT JOIN {{ ref('silver__contracts') }}
    c1
    ON l.token0 = c1.contract_address
    LEFT JOIN {{ ref('silver__contracts') }}
    c2
    ON l.token1 = c2.contract_address
    LEFT JOIN lp_token_pull lp
    ON l.tx_hash = lp.tx_hash
