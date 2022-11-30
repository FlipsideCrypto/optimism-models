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
price_provider_metadata AS (
    SELECT
        *
    FROM
        (
            SELECT
                id,
                token_address,
                2 AS priority
            FROM
                {{ source(
                    'silver_crosschain',
                    'asset_metadata_coin_market_cap'
                ) }}
            WHERE
                platform = 'Optimism'
            UNION ALL
            SELECT
                id,
                token_address,
                1 AS priority
            FROM
                {{ source(
                    'silver_crosschain',
                    'asset_metadata_coin_gecko'
                ) }}
            WHERE
                platform = 'optimistic-ethereum'
        ) qualify(ROW_NUMBER() over(PARTITION BY token_address
    ORDER BY
        priority ASC) = 1)
),
hourly_prices AS (
    SELECT
        p0.id :: STRING AS id,
        token_address,
        CLOSE AS price,
        recorded_hour AS HOUR,
        _inserted_timestamp
    FROM
        {{ source(
            'silver_crosschain',
            'hourly_prices_coin_market_cap'
        ) }}
        p0
        INNER JOIN price_provider_metadata
        ON p0.id :: STRING = price_provider_metadata.id :: STRING

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    p1.id :: STRING AS id,
    token_address,
    CLOSE AS price,
    recorded_hour AS HOUR,
    _inserted_timestamp
FROM
    {{ source(
        'silver_crosschain',
        'hourly_prices_coin_gecko'
    ) }}
    p1
    INNER JOIN price_provider_metadata
    ON p1.id :: STRING = price_provider_metadata.id :: STRING

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
),
hour_token_addresses_pair AS (
    SELECT
        DISTINCT p.hour,
        hp.token_address
    FROM
        {{ source(
            'ethereum',
            'fact_hourly_token_prices'
        ) }}
        p
        CROSS JOIN (
            SELECT
                DISTINCT token_address
            FROM
                hourly_prices
        ) hp

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
        p.price AS avg_price,
        LAG(
            p.price
        ) ignore nulls over (
            PARTITION BY h.token_address
            ORDER BY
                h.hour
        ) AS imputed_price,
        LAG(
            p._inserted_timestamp
        ) ignore nulls over (
            PARTITION BY h.token_address
            ORDER BY
                h.hour
        ) AS lag_inserted_timestamp,
        p._inserted_timestamp
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
        decimals,
        CASE
            WHEN avg_price IS NOT NULL THEN avg_price
            ELSE imputed_price
        END AS price,
        symbol,
        CASE
            WHEN avg_price IS NULL THEN TRUE
            ELSE FALSE
        END AS is_imputed,
        CASE
            WHEN _inserted_timestamp IS NOT NULL THEN _inserted_timestamp
            ELSE lag_inserted_timestamp
        END AS _inserted_timestamp
    FROM
        imputed p
        LEFT JOIN full_decimals
        ON LOWER(
            p.token_address
        ) = LOWER(
            full_decimals.contract_address
        )
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
            OR token_address = LOWER('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2')
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
            WHEN symbol IS NULL
            AND op_token_address = '0x9e5aac1ba1a2e6aed6b32689dfcf62a509ca96f3' THEN 'DF'
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
        is_imputed,
        _inserted_timestamp
    FROM
        FINAL
    UNION ALL
    SELECT
        HOUR,
        token_address,
        symbol,
        decimals,
        price,
        is_imputed,
        HOUR AS _inserted_timestamp
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
        COALESCE(
            token_address,
            'ETH'
        )
    ) AS price_id,
    _inserted_timestamp
FROM
    all_prices qualify(ROW_NUMBER() over(PARTITION BY price_id
ORDER BY
    _inserted_timestamp DESC) = 1)
