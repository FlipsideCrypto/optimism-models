-- depends_on: {{ ref('silver__complete_token_prices') }}
{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform','version'],
  cluster_by = ['block_timestamp::DATE'],
  tags = ['curated','reorg','heal']
) }}

WITH univ3 AS (

  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    pool_address AS contract_address,
    'Swap' AS event_name,
    CASE
      WHEN amount0_unadj > 0 THEN ABS(amount0_unadj)
      ELSE ABS(amount1_unadj)
    END AS amount_in_unadj,
    CASE
      WHEN amount0_unadj < 0 THEN ABS(amount0_unadj)
      ELSE ABS(amount1_unadj)
    END AS amount_out_unadj,
    CASE
      WHEN amount0_unadj > 0 THEN token0_address
      ELSE token1_address
    END AS token_in,
    CASE
      WHEN amount0_unadj < 0 THEN token0_address
      ELSE token1_address
    END AS token_out,
    sender,
    recipient AS tx_to,
    event_index,
    'uniswap-v3' AS platform,
    'v3' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__univ3_swaps') }}

{% if is_incremental() and 'univ3' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
univ2 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v2' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__univ2_swaps') }}

{% if is_incremental() and 'univ2' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
hashflow AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__hashflow_swaps') }}

{% if is_incremental() and 'hashflow' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
hashflow_v3 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v3' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__hashflow_v3_swaps') }}

{% if is_incremental() and 'hashflow_v3' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
velodrome AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    CASE
      WHEN event_name IS NULL THEN 'Swap'
      ELSE event_name
    END AS event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_address_in AS token_in,
    token_address_out AS token_out,
    sender_address AS sender,
    to_address AS tx_to,
    event_index,
    platform,
    'v1' AS version,
    _log_id,
    '1970-01-01' :: DATE AS _inserted_timestamp
  FROM
    {{ ref('velodrome__ez_swaps') }}

{% if is_incremental() and 'velodrome' not in var('HEAL_MODELS') %}
WHERE
  block_timestamp >= (
    SELECT
      MAX(block_timestamp) - INTERVAL '72 hours'
    FROM
      {{ this }}
  )
{% endif %}
),
sushi AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__sushi_swaps') }}

{% if is_incremental() and 'sushi' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
synthetix AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__synthetix_swaps') }}

{% if is_incremental() and 'synthetix' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
curve AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    tokens_sold AS amount_in_unadj,
    tokens_bought AS amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__curve_swaps') }}

{% if is_incremental() and 'curve' not in var('HEAL_MODELS') %}
AND _inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
  FROM
    {{ this }}
)
{% endif %}
),
beethovenx AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    CASE
      WHEN event_name IS NULL THEN 'Swap'
      ELSE event_name
    END AS event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__beethovenx_swaps') }}

{% if is_incremental() and 'beethovenx' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
kyberswap_v1_static AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v1-static' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__kyberswap_v1_static_swaps') }}

{% if is_incremental() and 'kyberswap_v1_static' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
kyberswap_v2_elastic AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v2' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__kyberswap_v2_elastic_swaps') }}

{% if is_incremental() and 'kyberswap_v2_elastic' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
fraxswap AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__fraxswap_swaps') }}

{% if is_incremental() and 'fraxswap' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
velodrome_v2 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v2' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__velodrome_v2_swaps') }}

{% if is_incremental() and 'velodrome_v2' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
woofi AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v1' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__woofi_swaps') }}

{% if is_incremental() and 'woofi' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
dodo_v2 AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v2' AS version,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__dodo_v2_swaps') }}

{% if is_incremental() and 'dodo_v2' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
--union all standard dex CTEs here (excludes amount_usd)
all_dex AS (
  SELECT
    *
  FROM
    sushi
  UNION ALL
  SELECT
    *
  FROM
    univ2
  UNION ALL
  SELECT
    *
  FROM
    synthetix
  UNION ALL
  SELECT
    *
  FROM
    curve
  UNION ALL
  SELECT
    *
  FROM
    beethovenx
  UNION ALL
  SELECT
    *
  FROM
    fraxswap
  UNION ALL
  SELECT
    *
  FROM
    velodrome_v2
  UNION ALL
  SELECT
    *
  FROM
    hashflow
  UNION ALL
  SELECT
    *
  FROM
    hashflow_v3
  UNION ALL
  SELECT
    *
  FROM
    woofi
  UNION ALL
  SELECT
    *
  FROM
    kyberswap_v1_static
  UNION ALL
  SELECT
    *
  FROM
    kyberswap_v2_elastic
  UNION ALL
  SELECT
    *
  FROM
    dodo_v2
  UNION ALL
  SELECT
    *
  FROM
    velodrome
  UNION ALL
  SELECT
    *
  FROM
    univ3
),
complete_dex_swaps AS (
  SELECT
    s.block_number,
    s.block_timestamp,
    s.tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    s.contract_address,
    event_name,
    token_in,
    c1.token_decimals AS decimals_in,
    c1.token_symbol AS symbol_in,
    amount_in_unadj,
    CASE
      WHEN decimals_in IS NULL THEN amount_in_unadj
      ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    CASE
      WHEN decimals_in IS NOT NULL THEN amount_in * p1.price
      ELSE NULL
    END AS amount_in_usd,
    token_out,
    c2.token_decimals AS decimals_out,
    c2.token_symbol AS symbol_out,
    amount_out_unadj,
    CASE
      WHEN decimals_out IS NULL THEN amount_out_unadj
      ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    CASE
      WHEN decimals_out IS NOT NULL THEN amount_out * p2.price
      ELSE NULL
    END AS amount_out_usd,
    CASE
      WHEN lp.pool_name IS NULL THEN CONCAT(
        LEAST(
          COALESCE(
            symbol_in,
            CONCAT(SUBSTRING(token_in, 1, 5), '...', SUBSTRING(token_in, 39, 42))
          ),
          COALESCE(
            symbol_out,
            CONCAT(SUBSTRING(token_out, 1, 5), '...', SUBSTRING(token_out, 39, 42))
          )
        ),
        '-',
        GREATEST(
          COALESCE(
            symbol_in,
            CONCAT(SUBSTRING(token_in, 1, 5), '...', SUBSTRING(token_in, 39, 42))
          ),
          COALESCE(
            symbol_out,
            CONCAT(SUBSTRING(token_out, 1, 5), '...', SUBSTRING(token_out, 39, 42))
          )
        )
      )
      ELSE lp.pool_name
    END AS pool_name,
    sender,
    tx_to,
    event_index,
    s.platform,
    s.version,
    s._log_id,
    s._inserted_timestamp
  FROM
    all_dex s
    LEFT JOIN {{ ref('silver__contracts') }}
    c1
    ON s.token_in = c1.contract_address
    LEFT JOIN {{ ref('silver__contracts') }}
    c2
    ON s.token_out = c2.contract_address
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p1
    ON s.token_in = p1.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p1.hour
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p2
    ON s.token_out = p2.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p2.hour
    LEFT JOIN {{ ref('silver_dex__complete_dex_liquidity_pools') }}
    lp
    ON s.contract_address = lp.pool_address
),

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
heal_model AS (
  SELECT
    t0.block_number,
    t0.block_timestamp,
    t0.tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    t0.contract_address,
    event_name,
    token_in,
    c1.token_decimals AS decimals_in,
    c1.token_symbol AS symbol_in,
    amount_in_unadj,
    CASE
      WHEN c1.token_decimals IS NULL THEN amount_in_unadj
      ELSE (amount_in_unadj / pow(10, c1.token_decimals))
    END AS amount_in_heal,
    CASE
      WHEN c1.token_decimals IS NOT NULL THEN amount_in_heal * p1.price
      ELSE NULL
    END AS amount_in_usd_heal,
    token_out,
    c2.token_decimals AS decimals_out,
    c2.token_symbol AS symbol_out,
    amount_out_unadj,
    CASE
      WHEN c2.token_decimals IS NULL THEN amount_out_unadj
      ELSE (amount_out_unadj / pow(10, c2.token_decimals))
    END AS amount_out_heal,
    CASE
      WHEN c2.token_decimals IS NOT NULL THEN amount_out_heal * p2.price
      ELSE NULL
    END AS amount_out_usd_heal,
    CASE
      WHEN lp.pool_name IS NULL THEN CONCAT(
        LEAST(
          COALESCE(
            c1.token_symbol,
            CONCAT(SUBSTRING(token_in, 1, 5), '...', SUBSTRING(token_in, 39, 42))
          ),
          COALESCE(
            c2.token_symbol,
            CONCAT(SUBSTRING(token_out, 1, 5), '...', SUBSTRING(token_out, 39, 42))
          )
        ),
        '-',
        GREATEST(
          COALESCE(
            c1.token_symbol,
            CONCAT(SUBSTRING(token_in, 1, 5), '...', SUBSTRING(token_in, 39, 42))
          ),
          COALESCE(
            c2.token_symbol,
            CONCAT(SUBSTRING(token_out, 1, 5), '...', SUBSTRING(token_out, 39, 42))
          )
        )
      )
      ELSE lp.pool_name
    END AS pool_name_heal,
    sender,
    tx_to,
    event_index,
    t0.platform,
    t0.version,
    t0._log_id,
    t0._inserted_timestamp
  FROM
    {{ this }}
    t0
    LEFT JOIN {{ ref('silver__contracts') }}
    c1
    ON t0.token_in = c1.contract_address
    LEFT JOIN {{ ref('silver__contracts') }}
    c2
    ON t0.token_out = c2.contract_address
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p1
    ON t0.token_in = p1.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p1.hour
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p2
    ON t0.token_out = p2.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p2.hour
    LEFT JOIN {{ ref('silver_dex__complete_dex_liquidity_pools') }}
    lp
    ON t0.contract_address = lp.pool_address
  WHERE
    CONCAT(
      t0.block_number,
      '-',
      t0.platform,
      '-',
      t0.version
    ) IN (
      SELECT
        CONCAT(
          t1.block_number,
          '-',
          t1.platform,
          '-',
          t1.version
        )
      FROM
        {{ this }}
        t1
      WHERE
        t1.decimals_in IS NULL
        AND t1._inserted_timestamp < (
          SELECT
            MAX(
              _inserted_timestamp
            ) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
          FROM
            {{ this }}
        )
        AND EXISTS (
          SELECT
            1
          FROM
            {{ ref('silver__contracts') }} C
          WHERE
            C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
            AND C.token_decimals IS NOT NULL
            AND C.contract_address = t1.token_in)
          GROUP BY
            1
        )
        OR CONCAT(
          t0.block_number,
          '-',
          t0.platform,
          '-',
          t0.version
        ) IN (
          SELECT
            CONCAT(
              t2.block_number,
              '-',
              t2.platform,
              '-',
              t2.version
            )
          FROM
            {{ this }}
            t2
          WHERE
            t2.decimals_out IS NULL
            AND t2._inserted_timestamp < (
              SELECT
                MAX(
                  _inserted_timestamp
                ) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
              FROM
                {{ this }}
            )
            AND EXISTS (
              SELECT
                1
              FROM
                {{ ref('silver__contracts') }} C
              WHERE
                C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                AND C.token_decimals IS NOT NULL
                AND C.contract_address = t2.token_out)
              GROUP BY
                1
            )
            OR CONCAT(
              t0.block_number,
              '-',
              t0.platform,
              '-',
              t0.version
            ) IN (
              SELECT
                CONCAT(
                  t3.block_number,
                  '-',
                  t3.platform,
                  '-',
                  t3.version
                )
              FROM
                {{ this }}
                t3
              WHERE
                t3.amount_in_usd IS NULL
                AND t3._inserted_timestamp < (
                  SELECT
                    MAX(
                      _inserted_timestamp
                    ) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
                  FROM
                    {{ this }}
                )
                AND EXISTS (
                  SELECT
                    1
                  FROM
                    {{ ref('silver__complete_token_prices') }}
                    p
                  WHERE
                    p._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                    AND p.price IS NOT NULL
                    AND p.token_address = t3.token_in
                    AND p.hour = DATE_TRUNC(
                      'hour',
                      t3.block_timestamp
                    )
                )
              GROUP BY
                1
            )
            OR CONCAT(
              t0.block_number,
              '-',
              t0.platform,
              '-',
              t0.version
            ) IN (
              SELECT
                CONCAT(
                  t4.block_number,
                  '-',
                  t4.platform,
                  '-',
                  t4.version
                )
              FROM
                {{ this }}
                t4
              WHERE
                t4.amount_out_usd IS NULL
                AND t4._inserted_timestamp < (
                  SELECT
                    MAX(
                      _inserted_timestamp
                    ) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
                  FROM
                    {{ this }}
                )
                AND EXISTS (
                  SELECT
                    1
                  FROM
                    {{ ref('silver__complete_token_prices') }}
                    p
                  WHERE
                    p._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                    AND p.price IS NOT NULL
                    AND p.token_address = t4.token_out
                    AND p.hour = DATE_TRUNC(
                      'hour',
                      t4.block_timestamp
                    )
                )
              GROUP BY
                1
            )
        ),
      {% endif %}

      FINAL AS (
        SELECT
          *
        FROM
          complete_dex_swaps

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  event_name,
  token_in,
  decimals_in,
  symbol_in,
  amount_in_unadj,
  amount_in_heal AS amount_in,
  amount_in_usd_heal AS amount_in_usd,
  token_out,
  decimals_out,
  symbol_out,
  amount_out_unadj,
  amount_out_heal AS amount_out,
  amount_out_usd_heal AS amount_out_usd,
  pool_name_heal AS pool_name,
  sender,
  tx_to,
  event_index,
  platform,
  version,
  _log_id,
  _inserted_timestamp
FROM
  heal_model
{% endif %}
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
  amount_in_unadj,
  amount_in,
  amount_in_usd,
  amount_out_unadj,
  amount_out,
  amount_out_usd,
  sender,
  tx_to,
  event_index,
  platform,
  version,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  decimals_in,
  decimals_out,
  _log_id,
  _inserted_timestamp,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_hash','event_index']
  ) }} AS complete_dex_swaps_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL qualify (ROW_NUMBER() over (PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1
