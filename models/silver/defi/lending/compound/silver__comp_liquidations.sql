{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated']
) }}

WITH comp_assets AS (
    SELECT
        compound_market_address,
        compound_market_name,
        compound_market_symbol,
        compound_market_decimals,
        underlying_asset_address,
        underlying_asset_name,
        underlying_asset_symbol,
        underlying_asset_decimals
    FROM {{ ref('silver__comp_asset_details') }}
),
liquidations AS (
    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.event_index,
        l.origin_from_address,
        l.origin_to_address,
        l.origin_function_signature,
        l.contract_address,
        regexp_substr_all(SUBSTR(l.DATA, 3, len(l.DATA)), '.{64}') AS segmented_data,
        l.contract_address AS compound_market,
        CONCAT('0x', SUBSTR(l.topics[3] :: STRING, 27, 40)) AS asset,
        CONCAT('0x', SUBSTR(l.topics[1] :: STRING, 27, 40)) AS absorber,
        CONCAT('0x', SUBSTR(l.topics[2] :: STRING, 27, 40)) AS borrower,
        utils.udf_hex_to_int(segmented_data[0] :: STRING) :: INTEGER AS collateral_absorbed,
        utils.udf_hex_to_int(segmented_data[1] :: STRING) :: INTEGER AS usd_value,
        l.origin_from_address AS depositor_address,
        'Compound V3' AS compound_version,
        C.token_name,
        C.token_symbol,
        C.token_decimals,
        'optimism' AS blockchain,
        CONCAT(l.tx_hash :: STRING, '-', l.event_index :: STRING) AS _log_id,
        l.modified_timestamp AS _inserted_timestamp
    FROM {{ ref('core__fact_event_logs') }} l
    LEFT JOIN {{ ref('silver__contracts') }} C ON asset = C.contract_address
    WHERE l.topics[0] = '0x9850ab1af75177e4a9201c65a2cf7976d5d28e40ef63494b44366f86b2f9412e'
      AND l.contract_address IN (
            SELECT DISTINCT compound_market_address FROM comp_assets
      )
      AND l.tx_succeeded
    {% if is_incremental() %}
      AND l.modified_timestamp >= (
        SELECT MAX(_inserted_timestamp) - INTERVAL '12 hours' FROM {{ this }}
      )
      AND l.modified_timestamp >= SYSDATE() - INTERVAL '7 day'
    {% endif %}
)
SELECT
    l.tx_hash,
    l.block_number,
    l.block_timestamp,
    l.event_index,
    l.origin_from_address,
    l.origin_to_address,
    l.origin_function_signature,
    l.contract_address,
    compound_market,
    absorber,
    borrower,
    depositor_address,
    asset AS token_address,
    token_symbol,
    collateral_absorbed AS amount_unadj,
    collateral_absorbed / pow(10, token_decimals) AS amount,
    usd_value / pow(10, 8) AS amount_usd,
    A.underlying_asset_address AS debt_asset,
    A.underlying_asset_symbol AS debt_asset_symbol,
    compound_version,
    blockchain,
    l._log_id,
    l._inserted_timestamp
FROM liquidations l
LEFT JOIN comp_assets A ON l.compound_market = A.compound_market_address
QUALIFY ROW_NUMBER() OVER (PARTITION BY l._log_id ORDER BY l._inserted_timestamp DESC) = 1
