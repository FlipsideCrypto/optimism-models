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
borrow AS (
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
        CONCAT('0x', SUBSTR(l.topics[1] :: STRING, 27, 40)) AS src_address,
        CONCAT('0x', SUBSTR(l.topics[2] :: STRING, 27, 40)) AS to_address,
        utils.udf_hex_to_int(segmented_data[0] :: STRING) :: INTEGER AS borrow_amount,
        l.origin_from_address AS borrower_address,
        'Compound V3' AS compound_version,
        C.compound_market_name AS NAME,
        C.compound_market_symbol AS symbol,
        C.compound_market_decimals AS decimals,
        C.underlying_asset_address,
        C.underlying_asset_symbol,
        'optimism' AS blockchain,
        CONCAT(l.tx_hash :: STRING, '-', l.event_index :: STRING) AS _log_id,
        l.modified_timestamp AS _inserted_timestamp
    FROM {{ ref('core__fact_event_logs') }} l
    LEFT JOIN comp_assets C ON l.contract_address = C.compound_market_address
    WHERE l.topics[0] = '0x9b1bfa7fa9ee420a16e124f794c35ac9f90472acc99140eb2f6447c714cad8eb'
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
    borrower_address AS borrower,
    underlying_asset_address AS token_address,
    underlying_asset_symbol AS token_symbol,
    borrow_amount AS amount_unadj,
    borrow_amount / pow(10, decimals) AS amount,
    symbol AS itoken_symbol,
    compound_version,
    blockchain,
    _log_id,
    _inserted_timestamp
FROM borrow
QUALIFY ROW_NUMBER() OVER (PARTITION BY _log_id ORDER BY _inserted_timestamp DESC) = 1
