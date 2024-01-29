{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH asset_details AS (

    SELECT
        borrowable1 AS token_address,
        token0 AS underlying_asset_address,
        token0_symbol AS underlying_asset_symbol,
        token0_decimals AS underlying_decimals
    FROM
        {{ ref('silver__tarot_liquidity_pools') }}
    GROUP BY
        ALL
    UNION ALL
    SELECT
        borrowable2 AS token_address,
        token1 AS underlying_asset_address,
        token1_symbol AS underlying_asset_symbol,
        token1_decimals AS underlying_decimals
    FROM
        {{ ref('silver__tarot_liquidity_pools') }}
    GROUP BY
        ALL
),
log_pull AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 25, 40)) AS sender,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 25, 40)) AS supplier,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS mintAmount_raw,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        contract_address IN (
            SELECT
                token_address
            FROM
                asset_details
        )
        AND topics [0] :: STRING = '0x2f00e3cdd69a77be7ed215ec7b2a36784dd158f921fca79ac29deffa353fe6ee'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    A.token_address,
    'bTAROT' AS token_symbol,
    mintAmount_raw AS amount_unadj,
    mintAmount_raw / pow(
        10,
        underlying_decimals
    ) AS amount,
    underlying_asset_address AS supplied_contract_addr,
    underlying_asset_symbol AS supplied_symbol,
    supplier,
    'Tarot' AS platform,
    _inserted_timestamp,
    _log_id
FROM
    log_pull l
    LEFT JOIN asset_details A
    ON A.token_address = l.contract_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
