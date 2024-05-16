{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = "block_number",
  cluster_by = ['block_timestamp::DATE'],
  tags = ['reorg','curated']
) }}
-- pull all token addresses and corresponding name
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
  FROM
    {{ ref('silver__exactly_asset_details') }}
),
exactly_deposits AS (
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
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS supplier,
    utils.udf_hex_to_int(
      segmented_data [1] :: STRING
    ) :: INTEGER AS minttokens_raw,
    utils.udf_hex_to_int(
      segmented_data [0] :: STRING
    ) :: INTEGER AS mintAmount_raw,
    'Exactly' AS platform,
    _inserted_timestamp,
    _log_id
  FROM
    {{ ref('silver__logs') }}
  WHERE
    contract_address IN (
      SELECT
        token_address
      FROM
        asset_details
    )
    AND topics [0] :: STRING = '0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7'
    AND tx_status = 'SUCCESS'

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
),
exactly_combine AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    supplier,
    minttokens_raw,
    mintAmount_raw,
    C.underlying_asset_address AS supplied_contract_addr,
    C.underlying_symbol AS supplied_symbol,
    C.token_address,
    C.token_symbol,
    C.token_decimals,
    C.underlying_decimals,
    b.platform,
    b._log_id,
    b._inserted_timestamp
  FROM
    exactly_deposits b
    LEFT JOIN asset_details C
    ON b.token_address = C.token_address
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
  minttokens_raw / pow(
    10,
    token_decimals
  ) AS issued_tokens,
  mintAmount_raw AS amount_unadj,
  mintAmount_raw / pow(
    10,
    underlying_decimals
  ) AS amount,
  supplied_contract_addr,
  supplied_symbol,
  supplier,
  platform,
  _inserted_timestamp,
  _log_id
FROM
  exactly_combine qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1
