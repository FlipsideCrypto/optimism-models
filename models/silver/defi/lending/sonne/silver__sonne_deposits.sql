{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = "block_number",
  cluster_by = ['block_timestamp::DATE'],
  tags = ['silver','defi','lending','curated']
) }}

WITH asset_details AS (
  SELECT
    token_address,
    token_symbol,
    token_name,
    token_decimals,
    underlying_asset_address,
    underlying_name,
    underlying_symbol,
    underlying_decimals
  FROM {{ ref('silver__sonne_asset_details') }}
),
sonne_deposits AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    contract_address AS token_address,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    utils.udf_hex_to_int(segmented_data[2] :: STRING) :: INTEGER AS minttokens_raw,
    utils.udf_hex_to_int(segmented_data[1] :: STRING) :: INTEGER AS mintAmount_raw,
    CONCAT('0x', SUBSTR(segmented_data[0] :: STRING, 25, 40)) AS supplier,
    'Sonne' AS platform,
    modified_timestamp AS _inserted_timestamp,
    CONCAT(tx_hash :: STRING, '-', event_index :: STRING) AS _log_id
  FROM {{ ref('core__fact_event_logs') }}
  WHERE contract_address IN (
      SELECT token_address FROM asset_details
    )
    AND topics[0] :: STRING = '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f'
    AND tx_succeeded
    {% if is_incremental() %}
      AND modified_timestamp >= (
        SELECT MAX(_inserted_timestamp) - INTERVAL '12 hours' FROM {{ this }}
      )
      AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
    {% endif %}
),
sonne_combine AS (
  SELECT
    b.block_number,
    b.block_timestamp,
    b.tx_hash,
    b.event_index,
    b.origin_from_address,
    b.origin_to_address,
    b.origin_function_signature,
    b.contract_address,
    b.supplier,
    b.minttokens_raw,
    b.mintAmount_raw,
    C.underlying_asset_address AS supplied_contract_addr,
    C.underlying_symbol AS supplied_symbol,
    C.token_address,
    C.token_symbol,
    C.token_decimals,
    C.underlying_decimals,
    b.platform,
    b._log_id,
    b._inserted_timestamp
  FROM sonne_deposits b
  LEFT JOIN asset_details C ON b.token_address = C.token_address
)
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  event_index,
  origin_from_address,
  origin_to_address,
  origin_function_signature,
  contract_address,
  token_address,
  token_symbol,
  minttokens_raw / pow(10, token_decimals) AS issued_tokens,
  mintAmount_raw AS amount_unadj,
  mintAmount_raw / pow(10, underlying_decimals) AS amount,
  supplied_contract_addr,
  supplied_symbol,
  supplier,
  platform,
  _inserted_timestamp,
  _log_id
FROM sonne_combine
QUALIFY ROW_NUMBER() OVER (PARTITION BY _log_id ORDER BY _inserted_timestamp DESC) = 1
