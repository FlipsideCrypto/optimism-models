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
repayments AS (
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
        l.contract_address AS asset,
        CONCAT('0x', SUBSTR(l.topics[1] :: STRING, 27, 40)) AS repayer,
        CONCAT('0x', SUBSTR(l.topics[2] :: STRING, 27, 40)) AS borrower,
        utils.udf_hex_to_int(segmented_data[0] :: STRING) :: INTEGER AS amount,
        utils.udf_hex_to_int(segmented_data[1] :: STRING) :: INTEGER AS usd_value,
        l.origin_from_address AS depositor,
        'Compound V3' AS compound_version,
        C.compound_market_name,
        C.compound_market_symbol,
        C.compound_market_decimals,
        C.underlying_asset_address AS underlying_asset,
        C.underlying_asset_symbol,
        'optimism' AS blockchain,
        CONCAT(l.tx_hash :: STRING, '-', l.event_index :: STRING) AS _log_id,
        l.modified_timestamp AS _inserted_timestamp
    FROM {{ ref('core__fact_event_logs') }} l
    LEFT JOIN comp_assets C ON l.contract_address = C.compound_market_address
    WHERE l.topics[0] = '0xd1cf3d156d5f8f0d50f6c122ed609cec09d35c9b9fb3fff6ea0959134dae424e'
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
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    asset AS compound_market,
    repayer,
    borrower,
    depositor,
    underlying_asset AS token_address,
    underlying_asset_symbol AS token_symbol,
    amount AS amount_unadj,
    amount / pow(10, compound_market_decimals) AS amount,
    compound_version,
    blockchain,
    _log_id,
    _inserted_timestamp
FROM repayments
QUALIFY ROW_NUMBER() OVER (PARTITION BY _log_id ORDER BY _inserted_timestamp DESC) = 1
