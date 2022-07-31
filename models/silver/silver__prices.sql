{{ config(
    materialized = 'incremental',
    unique_key = 'price_id',
    cluster_by = ['hour::DATE', 'token_address']
) }}

WITH full_decimals AS (

    SELECT
        LOWER(address) AS contract_address,
        decimals,
        symbol
    FROM
        {{ ref('core__dim_contracts') }}
),
op_token_metadata AS (
    SELECT
        asset_id,
        NAME,
        b.symbol AS symbol,
        LOWER(token_address) AS token_address,
        decimals
    FROM
        {{ source(
            'legacy_silver',
            'market_asset_metadata'
        ) }} A
        LEFT JOIN full_decimals b
        ON LOWER(
            A.token_address
        ) = LOWER(
            b.contract_address
        )
    WHERE
        platform = 'optimism-ethereum'
        AND token_address IS NOT NULL
        AND b.symbol IS NOT NULL
),
hourly_prices AS (
    SELECT
        DATE_TRUNC(
            'hour',
            recorded_at
        ) AS HOUR,
        b.name AS NAME,
        b.symbol AS symbol,
        b.decimals AS decimals,
        token_address,
        AVG(price) AS price,
        AVG(total_supply) AS total_supply
    FROM
        {{ source(
            'legacy_silver',
            'prices_v2'
        ) }} A
        INNER JOIN op_token_metadata b
        ON A.asset_id = b.asset_id
    WHERE
        provider = 'coinmarketcap'

{% if is_incremental() %}
AND recorded_at >= CURRENT_DATE - 3
{% else %}
    AND recorded_at >= '2020-05-05' -- first date with valid prices data
{% endif %}
GROUP BY
    1,
    2,
    3,
    4,
    5
),
hour_token_addresses_pair AS (
    SELECT
        *
    FROM
        {{ source(
            'legacy_silver',
            'hours'
        ) }}
        CROSS JOIN (
            SELECT
                DISTINCT token_address
            FROM
                hourly_prices
        )

{% if is_incremental() %}
WHERE
    HOUR BETWEEN CURRENT_DATE - 3
    AND DATE_TRUNC('hour', SYSDATE())
{% else %}
WHERE
    HOUR BETWEEN '2020-05-05'
    AND DATE_TRUNC('hour', SYSDATE()) -- first date with valid prices data
{% endif %}),
imputed AS (
    SELECT
        h.hour,
        h.token_address,
        p.symbol,
        p.decimals,
        p.price AS avg_price,
        LAG(
            p.symbol
        ) ignore nulls over (
            PARTITION BY h.token_address
            ORDER BY
                h.hour
        ) AS lag_symbol,
        LAG(
            p.decimals
        ) ignore nulls over (
            PARTITION BY h.token_address
            ORDER BY
                h.hour
        ) AS lag_decimals,
        LAG(
            p.price
        ) ignore nulls over (
            PARTITION BY h.token_address
            ORDER BY
                h.hour
        ) AS imputed_price
    FROM
        hour_token_addresses_pair h
        LEFT OUTER JOIN hourly_prices p
        ON p.hour = h.hour
        AND (
            p.token_address = h.token_address
        )
),
FINAL AS (
    SELECT
        p.hour AS HOUR,
        p.token_address,
        CASE
            WHEN decimals IS NOT NULL THEN decimals
            ELSE lag_decimals
        END AS decimals,
        CASE
            WHEN avg_price IS NOT NULL THEN avg_price
            ELSE imputed_price
        END AS price,
        CASE
            WHEN symbol IS NOT NULL THEN symbol
            ELSE lag_symbol
        END AS symbol,
        CASE
            WHEN avg_price IS NULL THEN TRUE
            ELSE FALSE
        END AS is_imputed
    FROM
        imputed p
    WHERE
        price IS NOT NULL
),
eth_tokens AS (
    SELECT
        DISTINCT LOWER(eth_token_address) AS eth_token_address,
        LOWER(op_token_address) AS op_token_address
    FROM
        {{ ref('silver__velo_tokens_backup') }}
),
eth_token_prices AS (
    SELECT
        *
    FROM
        {{ source(
            'ethereum',
            'fact_hourly_token_prices'
        ) }}
    WHERE
        (
            token_address IN (
                SELECT
                    eth_token_address
                FROM
                    eth_tokens
            )
            OR token_address IS NULL
        )

{% if is_incremental() %}
AND HOUR BETWEEN CURRENT_DATE - 3
AND DATE_TRUNC('hour', SYSDATE())
{% else %}
    AND HOUR BETWEEN '2020-05-05'
    AND DATE_TRUNC('hour', SYSDATE()) -- first date with valid prices data
{% endif %}),
adj_eth_prices AS (
    SELECT
        HOUR,
        op_token_address AS token_address,
        CASE
            WHEN symbol IS NULL
            AND op_token_address = '0x296f55f8fb28e498b858d0bcda06d955b2cb3f97' THEN 'STG'
            WHEN symbol IS NULL
            AND op_token_address = '0x5029c236320b8f15ef0a657054b84d90bfbeded3' THEN 'BitANT'
            WHEN token_address IS NULL
            AND symbol IS NULL THEN 'ETH'
            ELSE symbol
        END AS symbol,
        decimals,
        price,
        is_imputed
    FROM
        eth_token_prices
        LEFT JOIN eth_tokens
        ON eth_token_address = token_address
),
all_prices AS (
    SELECT
        HOUR,
        token_address,
        symbol,
        decimals,
        price,
        is_imputed
    FROM
        FINAL
    UNION ALL
    SELECT
        HOUR,
        token_address,
        symbol,
        decimals,
        price,
        is_imputed
    FROM
        adj_eth_prices
)
SELECT
    HOUR,
    token_address,
    symbol,
    decimals,
    price,
    is_imputed,
    CONCAT(
        HOUR,
        token_address
    ) AS price_id
FROM
    all_prices qualify(ROW_NUMBER() over(PARTITION BY price_id
ORDER BY
    decimals DESC) = 1)
