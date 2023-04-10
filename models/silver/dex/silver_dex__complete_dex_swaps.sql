{{ config(
    materialized = 'incremental',
    unique_key = "_log_id",
    cluster_by = ['block_timestamp::DATE']
) }}

WITH contracts AS (

    SELECT
        address,
        symbol,
        NAME,
        decimals
    FROM
        {{ ref('core__dim_contracts') }}
),

prices AS (

    SELECT
        hour,
        token_address,
        price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        token_address IN (
            SELECT
                DISTINCT address
            FROM
                contracts
        )
{% if is_incremental() %}
AND hour >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 2
    FROM
      {{ this }}
  )  
{% endif %}
), 

univ3_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    pool_address AS contract_address,
    CONCAT(c1.symbol,'-',c2.symbol,' ',fee,' ',tick_spacing) AS pool_name,
    'Swap' AS event_name,
    amount0_unadj / pow(10,COALESCE(c1.decimals,18)) AS amount0_adjusted,
    amount1_unadj / pow(10,COALESCE(c2.decimals,18)) AS amount1_adjusted,
    CASE
        WHEN c1.decimals IS NOT NULL THEN ROUND(
            p1.price * amount0_adjusted,
            2
        )
    END AS amount0_usd,
    CASE
        WHEN c2.decimals IS NOT NULL THEN ROUND(
            p2.price * amount1_adjusted,
            2
        )
    END AS amount1_usd,
    CASE
      WHEN amount0_unadj > 0 THEN ABS(amount0_adjusted)
      ELSE ABS(amount1_adjusted)
    END AS amount_in,
    CASE
      WHEN amount0_unadj > 0 THEN ABS(amount0_usd)
      ELSE ABS(amount1_usd)
    END AS amount_in_usd,
    CASE
      WHEN amount0_unadj < 0 THEN ABS(amount0_adjusted)
      ELSE ABS(amount1_adjusted)
    END AS amount_out,
    CASE
      WHEN amount0_unadj < 0 THEN ABS(amount0_usd)
      ELSE ABS(amount1_usd)
    END AS amount_out_usd,
    sender,
    recipient AS tx_to,
    event_index,
    'uniswap-v3' AS platform,
    CASE
      WHEN amount0_unadj > 0 THEN token0_address
      ELSE token1_address
    END AS token_in,
    CASE
      WHEN amount0_unadj < 0 THEN token0_address
      ELSE token1_address
    END AS token_out,
    CASE
      WHEN amount0_unadj > 0 THEN c1.symbol
      ELSE c2.symbol
    END AS symbol_in,
    CASE
      WHEN amount0_unadj < 0 THEN c1.symbol
      ELSE c2.symbol
    END AS symbol_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__univ3_swaps') }} s
  LEFT JOIN contracts c1
    ON c1.address = s.token0_address
  LEFT JOIN contracts c2
    ON c2.address = s.token1_address
  LEFT JOIN prices p1
    ON s.token0_address = p1.token_address
      AND DATE_TRUNC('hour', block_timestamp) = p1.hour
  LEFT JOIN prices p2
    ON s.token1_address = p2.token_address
      AND DATE_TRUNC('hour', block_timestamp) = p2.hour
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE
    FROM
      {{ this }}
  )
{% endif %}
),

velodrome_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    CASE 
      WHEN event_name IS NULL THEN 'Swap' 
      ELSE event_name 
    END AS event_name,
    amount_in,
    amount_in_usd,
    amount_out,
    amount_out_usd,
    sender_address AS sender,
    to_address AS tx_to,
    event_index,
    platform,
    token_address_in AS token_in,
    token_address_out AS token_out,
    symbol_in,
    symbol_out,
    _log_id,
    '1970-01-01' :: DATE AS _inserted_timestamp
  FROM
    {{ ref('velodrome__ez_swaps') }} s
),
sushi_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    CASE 
      WHEN event_name IS NULL THEN 'Swap' 
      ELSE event_name 
    END AS event_name,
    amount_in,
    amount_out,
    origin_from_address AS sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    decimals_in,
    decimals_out,
    symbol_in,
    symbol_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__sushi_swaps') }} s
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE
    FROM
      {{ this }}
  )
{% endif %}
),
synthetix_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    COALESCE(decimals_in,18) AS decimals_in,
    COALESCE(decimals_out,18) AS decimals_out,
    CASE
        WHEN decimals_in IS NOT NULL THEN amount_in_unadj / pow(10,decimals_in)
        ELSE amount_in_unadj
    END AS amount_in,
    CASE
        WHEN decimals_out IS NOT NULL THEN amount_out_unadj / pow(10,decimals_out)
        ELSE amount_out_unadj
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    _log_id,
    _inserted_timestamp
  FROM {{ ref('silver_dex__synthetix_swaps')}} s
  LEFT JOIN (
        SELECT
            synth_symbol AS synth_symbol_in,
            synth_proxy_address AS token_in,
            decimals AS decimals_in
        FROM
            {{ ref('silver__synthetix_synths_20230404') }}
    ) synths_in
    ON synths_in.synth_symbol_in = s.symbol_in
  LEFT JOIN (
        SELECT
            synth_symbol AS synth_symbol_out,
            synth_proxy_address AS token_out,
            decimals AS decimals_out
        FROM
            {{ ref('silver__synthetix_synths_20230404') }}
    ) synths_out
    ON synths_out.synth_symbol_out = s.symbol_out
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE
    FROM
      {{ this }}
  )
{% endif %}
),
curve_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    CASE 
      WHEN event_name IS NULL THEN 'Swap'
      ELSE event_name
    END AS event_name,
    token_in,
    token_out,
    c1.symbol AS symbol_in,
    c2.symbol AS symbol_out,
    c1.decimals AS decimals_in,
    c2.decimals AS decimals_out,
     CASE
        WHEN decimals_in IS NOT NULL THEN tokens_sold / pow(
            10,
            decimals_in
        )
        ELSE tokens_sold
    END AS amount_in,
    CASE
        WHEN decimals_out IS NOT NULL THEN tokens_bought / pow(
            10,
            decimals_out
        )
        ELSE tokens_bought
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__curve_swaps') }} s
  LEFT JOIN contracts c1
    ON c1.address = s.token_in
  LEFT JOIN contracts c2
    ON c2.address = s.token_out
  WHERE symbol_out <> symbol_in
{% if is_incremental() %}
AND
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE
    FROM
      {{ this }}
  )
{% endif %}
),
beethovenx_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    CASE 
      WHEN event_name IS NULL THEN 'Swap'
      ELSE event_name
    END AS event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    CASE
        WHEN decimals_in IS NULL THEN amount_in_unadj
        ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    CASE
        WHEN decimals_out IS NULL THEN amount_out_unadj
        ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__beethovenx_swaps') }} s
  LEFT JOIN contracts c1
    ON s.token_in = c1.address
  LEFT JOIN contracts c2
    ON s.token_out = c2.address
{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE
    FROM
      {{ this }}
  )
{% endif %}
),

--union all standard dex CTEs here (excludes amount_usd)
all_dex_standard AS (
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_out,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  decimals_in,
  decimals_out,
  _log_id,
  _inserted_timestamp
FROM
  sushi_swaps
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_out,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  decimals_in,
  decimals_out,
  _log_id,
  _inserted_timestamp
FROM
  synthetix_swaps
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_out,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  decimals_in,
  decimals_out,
  _log_id,
  _inserted_timestamp
FROM
  curve_swaps
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_out,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  decimals_in,
  decimals_out,
  _log_id,
  _inserted_timestamp
FROM
  beethovenx_swaps
),

--union all non-standard dex CTEs here (excludes amount_usd)
all_dex_custom AS (
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_in_usd,
  amount_out,
  amount_out_usd,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  _log_id,
  _inserted_timestamp
FROM
  velodrome_swaps
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_in_usd,
  amount_out,
  amount_out_usd,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  _log_id,
  _inserted_timestamp
FROM
  univ3_swaps
),

FINAL AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in,
    CASE
        WHEN s.decimals_in IS NOT NULL THEN ROUND(
            amount_in * p1.price, 2)
        ELSE NULL
    END AS amount_in_usd,
    amount_out,
    CASE
        WHEN s.decimals_out IS NOT NULL THEN ROUND(
            amount_out * p2.price, 2)
        ELSE NULL
    END AS amount_out_usd,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    _log_id,
    _inserted_timestamp
  FROM all_dex_standard s
  LEFT JOIN prices p1
    ON s.token_in = p1.token_address
      AND DATE_TRUNC('hour', block_timestamp) = p1.hour
  LEFT JOIN prices p2
    ON s.token_out = p2.token_address
      AND DATE_TRUNC('hour', block_timestamp) = p2.hour
  UNION ALL
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in,
    ROUND(
      amount_in_usd, 2) AS amount_in_usd,
    amount_out,
    ROUND(
      amount_out_usd, 2) AS amount_out_usd,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    _log_id,
    _inserted_timestamp
  FROM
    all_dex_custom c
)

SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  CASE
    WHEN ABS((amount_in_usd - amount_out_usd) / NULLIF(amount_out_usd, 0)) > 0.5
      OR ABS((amount_in_usd - amount_out_usd) / NULLIF(amount_in_usd, 0)) > 0.5 THEN NULL
    ELSE amount_in_usd
  END AS amount_in_usd,
  amount_out,
  CASE
    WHEN ABS((amount_out_usd - amount_in_usd) / NULLIF(amount_in_usd, 0)) > 0.5
      OR ABS((amount_out_usd - amount_in_usd) / NULLIF(amount_out_usd, 0)) > 0.5 THEN NULL
    ELSE amount_out_usd
  END AS amount_out_usd,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  _log_id,
  _inserted_timestamp
FROM FINAL