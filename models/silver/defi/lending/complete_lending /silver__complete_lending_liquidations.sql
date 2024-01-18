{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform'],
  cluster_by = ['block_timestamp::DATE'],
  tags = ['non_realtime','reorg','curated']
) }}

WITH liquidation_union AS (

  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    liquidator,
    borrower,
    amount_unadj,
    liquidation_amount AS liquidated_amount,
    NULL AS liquidated_amount_usd,
    itoken AS protocol_collateral_asset,
    liquidation_contract_address AS collateral_asset,
    liquidation_contract_symbol AS collateral_asset_symbol,
    collateral_token AS debt_asset,
    collateral_symbol AS debt_asset_symbol,
    platform,
    'arbitrum' AS blockchain,
    l._LOG_ID,
    l._INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__lodestar_liquidations') }}
    l

{% if is_incremental() %}
WHERE
  l._inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
UNION ALL
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    liquidator,
    borrower,
    amount_unadj,
    amount AS liquidated_amount,
    NULL AS liquidated_amount_usd,
    token AS protocol_collateral_asset,
    liquidation_contract_address AS collateral_asset,
    liquidation_contract_symbol AS collateral_asset_symbol,
    collateral_token AS debt_asset,
    collateral_symbol AS debt_asset_symbol,
    platform,
    'arbitrum' AS blockchain,
    l._LOG_ID,
    l._INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__dforce_liquidations') }}
    l

{% if is_incremental() %}
WHERE
  l._inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
UNION ALL
SELECT
  tx_hash,
  block_number,
  block_timestamp,
  event_index,
  origin_from_address,
  origin_to_address,
  origin_function_signature,
  contract_address,
  liquidator,
  borrower,
  amount_unadj,
  amount AS liquidated_amount,
  NULL AS liquidated_amount_usd,
  collateral_aave_token AS protocol_collateral_asset,
  collateral_asset,
  collateral_token_symbol AS collateral_asset_symbol,
  debt_asset,
  debt_token_symbol AS debt_asset_symbol,
  'Aave V3' AS platform,
  'arbitrum' AS blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__aave_liquidations') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
      {{ this }}
  )
{% endif %}
UNION ALL
SELECT
  tx_hash,
  block_number,
  block_timestamp,
  event_index,
  origin_from_address,
  origin_to_address,
  origin_function_signature,
  contract_address,
  liquidator,
  borrower,
  amount_unadj,
  amount AS liquidated_amount,
  NULL AS liquidated_amount_usd,
  collateral_radiant_token AS protocol_collateral_asset,
  collateral_asset,
  collateral_token_symbol AS collateral_asset_symbol,
  debt_asset,
  debt_token_symbol AS debt_asset_symbol,
  platform,
  'arbitrum' AS blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__radiant_liquidations') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
UNION ALL
SELECT
  tx_hash,
  block_number,
  block_timestamp,
  event_index,
  origin_from_address,
  origin_to_address,
  origin_function_signature,
  contract_address,
  receiver_address AS liquidator,
  depositor_address AS borrower,
  amount_unadj,
  amount AS liquidated_amount,
  NULL AS liquidated_amount_usd,
  protocol_collateral_token AS protocol_collateral_asset,
  token_address AS collateral_asset,
  token_symbol AS collateral_asset_symbol,
  debt_asset,
  debt_asset_symbol,
  platform,
  'arbitrum' AS blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__silo_liquidations') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
UNION ALL
SELECT
  tx_hash,
  block_number,
  block_timestamp,
  event_index,
  origin_from_address,
  origin_to_address,
  origin_function_signature,
  contract_address,
  absorber AS liquidator,
  borrower,
  amount_unadj,
  amount AS liquidated_amount,
  amount_usd AS liquidated_amount_usd,
  compound_market AS protocol_collateral_asset,
  token_address AS collateral_asset,
  token_symbol AS collateral_asset_symbol,
  debt_asset,
  debt_asset_symbol,
  l.compound_version AS platform,
  'arbitrum' AS blockchain,
  l._LOG_ID,
  l._INSERTED_TIMESTAMP
FROM
  {{ ref('silver__comp_liquidations') }}
  l

{% if is_incremental() %}
WHERE
  l._inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
),
contracts AS (
  SELECT
    *
  FROM
    {{ ref('silver__contracts') }} C
  WHERE
    C.contract_address IN (
      SELECT
        DISTINCT(collateral_asset) AS asset
      FROM
        liquidation_union
    )
),
prices AS (
  SELECT
    *
  FROM
    {{ ref('price__ez_hourly_token_prices') }}
    p
  WHERE
    token_address IN (
      SELECT
        DISTINCT(collateral_asset) AS asset
      FROM
        liquidation_union
    )
    AND HOUR > (
      SELECT
        MIN(block_timestamp)
      FROM
        liquidation_union
    )
),
FINAL AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    A.contract_address,
    CASE
      WHEN platform = 'Compound V3' THEN 'AbsorbCollateral'
      WHEN platform = 'Lodestar' THEN 'LiquidateBorrow'
      WHEN platform = 'Silo' THEN 'Liquidate'
      ELSE 'LiquidationCall'
    END AS event_name,
    liquidator,
    borrower,
    protocol_collateral_asset AS protocol_market,
    collateral_asset AS collateral_token,
    collateral_asset_symbol AS collateral_token_symbol,
    amount_unadj,
    liquidated_amount AS amount,
    CASE
      WHEN platform <> 'Compound V3' THEN ROUND(
        liquidated_amount * p.price,
        2
      )
      ELSE ROUND(
        liquidated_amount_usd,
        2
      )
    END AS amount_usd,
    debt_asset AS debt_token,
    debt_asset_symbol AS debt_token_symbol,
    platform,
    A.blockchain,
    A._LOG_ID,
    A._INSERTED_TIMESTAMP
  FROM
    liquidation_union A
    LEFT JOIN prices p
    ON collateral_asset = p.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p.hour
    LEFT JOIN contracts C
    ON collateral_asset = C.contract_address
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS complete_lending_liquidations_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1
