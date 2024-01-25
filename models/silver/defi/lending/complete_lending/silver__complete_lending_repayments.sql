{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform'],
  cluster_by = ['block_timestamp::DATE'],
  tags = ['non_realtime','reorg','curated']
) }}

WITH aave_repayments AS (

  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    aave_market AS token_address,
    aave_token AS protocol_market,
    amount_unadj,
    amount,
    symbol AS token_symbol,
    payer AS payer_address,
    borrower,
    'Aave V3' AS platform,
    'optimism' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__aave_repayments') }}
{% if is_incremental() and 'aave' not in var('HEAL_CURATED_MODEL') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
),

granary_repayments as (

  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    granary_market AS token_address,
    granary_token AS protocol_market,
    amount_unadj,
    amount,
    symbol AS token_symbol,
    payer AS payer_address,
    borrower,
    platform,
    'optimism' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__granary_repayments') }}

  {% if is_incremental() and 'granary' not in var('HEAL_CURATED_MODEL') %}
  WHERE
    _inserted_timestamp >= (
      SELECT
        MAX(_inserted_timestamp) - INTERVAL '36 hours'
      FROM
        {{ this }}
    )
  {% endif %}
),

exactly_repayments as (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    repay_contract_address AS token_address,
    token_address AS protocol_market,
    amount_unadj,
    amount,
    repay_contract_symbol AS token_symbol,
    payer AS payer_address,
    borrower,
    platform,
    'optimism' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__exactly_repayments') }}

  {% if is_incremental() and 'exactly' not in var('HEAL_CURATED_MODEL') %}
  WHERE
    _inserted_timestamp >= (
      SELECT
        MAX(_inserted_timestamp) - INTERVAL '36 hours'
      FROM
        {{ this }}
    )
  {% endif %}
),

sonne_repayments as (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    repay_contract_address AS token_address,
    token_address AS protocol_market,
    amount_unadj,
    amount,
    repay_contract_symbol AS token_symbol,
    payer AS payer_address,
    borrower,
    platform,
    'optimism' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__sonne_repayments') }}

 {% if is_incremental() and 'sonne' not in var('HEAL_CURATED_MODEL') %}
  WHERE
    _inserted_timestamp >= (
      SELECT
        MAX(_inserted_timestamp) - INTERVAL '36 hours'
      FROM
        {{ this }}
    )
  {% endif %}
),

tarot_repayments as (

  SELECT
      tx_hash,
      block_number,
      block_timestamp,
      event_index,
      origin_from_address,
      origin_to_address,
      origin_function_signature,
      contract_address,
      repay_contract_address AS token_address,
      token_address AS protocol_market,
      amount_unadj,
      amount,
      repay_contract_symbol AS token_symbol,
      payer AS payer_address,
      borrower,
      platform,
      'optimism' AS blockchain,
      _LOG_ID,
      _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__tarot_repayments') }}

{% if is_incremental() and 'tarot' not in var('HEAL_CURATED_MODEL') %}
  WHERE
    _inserted_timestamp >= (
      SELECT
        MAX(_inserted_timestamp) - INTERVAL '36 hours'
      FROM
        {{ this }}
    )
  {% endif %}

),

repayments_union as (
    SELECT
        *
    FROM
        aave_repayments
    UNION ALL
    SELECT
        *
    FROM
        granary_repayments
    UNION ALL
    SELECT
        *
    FROM
        exactly_repayments
    UNION ALL
    SELECT
        *
    FROM
        sonne_repayments
    UNION ALL
    SELECT
        *
    FROM
        tarot_repayments
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
      WHEN platform = 'Sonne' THEN 'RepayBorrow'
      ELSE 'Repay'
    END AS event_name,
    protocol_market,
    payer_address AS payer,
    borrower,
    A.token_address,
    A.token_symbol,
    amount_unadj,
    amount,
    ROUND(
      amount * price,
      2
    ) AS amount_usd,
    platform,
    blockchain,
    A._LOG_ID,
    A._INSERTED_TIMESTAMP
  FROM
    repayments_union A
    LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
    p
    ON A.token_address = p.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p.hour
    LEFT JOIN {{ ref('silver__contracts') }} C
    ON A.token_address = C.contract_address
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS complete_lending_repayments_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1
