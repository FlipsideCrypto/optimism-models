{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'VELODROME',
    'PURPOSE': 'DEFI, DEX, SWAPS' } } }
) }}

WITH velo_pools AS (

    SELECT
        pool_address,
        pool_name,
        pool_type,
        token0_symbol,
        token1_symbol,
        token0_address,
        token1_address,
        token0_decimals,
        token1_decimals
    FROM
        {{ ref('silver__velodrome_pools') }}
),
tokens AS (
    SELECT
        DISTINCT token0_address AS token_address,
        token0_decimals AS token_decimals,
        token0_symbol AS token_symbol
    FROM
        velo_pools
    UNION
    SELECT
        DISTINCT token1_address AS token_address,
        token1_decimals AS token_decimals,
        token1_symbol AS token_symbol
    FROM
        velo_pools
)
SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    platform,
    contract_address,
    pool_address,
    pool_name,
    sender_address,
    to_address,
    CASE
        WHEN amount0_in_unadj <> 0 THEN amount0_in_unadj
        WHEN amount1_in_unadj <> 0 THEN amount1_in_unadj
    END AS amount_in_unadj,
    CASE
        WHEN (
            CASE
                WHEN amount0_in_unadj <> 0 THEN token0_decimals
                WHEN amount1_in_unadj <> 0 THEN token1_decimals
            END
        ) IS NOT NULL THEN (
            CASE
                WHEN amount0_in_unadj <> 0 THEN amount0_in_unadj
                WHEN amount1_in_unadj <> 0 THEN amount1_in_unadj
            END
        ) / pow(
            10,
            CASE
                WHEN amount0_in_unadj <> 0 THEN token0_decimals
                WHEN amount1_in_unadj <> 0 THEN token1_decimals
            END
        )
        ELSE (
            CASE
                WHEN amount0_in_unadj <> 0 THEN amount0_in_unadj
                WHEN amount1_in_unadj <> 0 THEN amount1_in_unadj
            END
        )
    END AS amount_in,
    CASE
        WHEN (
            CASE
                WHEN amount0_in_unadj <> 0 THEN token0_decimals
                WHEN amount1_in_unadj <> 0 THEN token1_decimals
            END
        ) IS NOT NULL THEN ROUND(
            amount_in * p0.price,
            2
        )
    END AS amount_in_usd,
    CASE
        WHEN amount0_out_unadj <> 0 THEN amount0_out_unadj
        WHEN amount1_out_unadj <> 0 THEN amount1_out_unadj
    END AS amount_out_unadj,
    CASE
        WHEN (
            CASE
                WHEN amount0_out_unadj <> 0 THEN token0_decimals
                WHEN amount1_out_unadj <> 0 THEN token1_decimals
            END
        ) IS NOT NULL THEN (
            CASE
                WHEN amount0_out_unadj <> 0 THEN amount0_out_unadj
                WHEN amount1_out_unadj <> 0 THEN amount1_out_unadj
            END
        ) / pow(
            10,
            CASE
                WHEN amount0_out_unadj <> 0 THEN token0_decimals
                WHEN amount1_out_unadj <> 0 THEN token1_decimals
            END
        )
        ELSE (
            CASE
                WHEN amount0_out_unadj <> 0 THEN amount0_out_unadj
                WHEN amount1_out_unadj <> 0 THEN amount1_out_unadj
            END
        )
    END AS amount_out,
    CASE
        WHEN (
            CASE
                WHEN amount0_out_unadj <> 0 THEN token0_decimals
                WHEN amount1_out_unadj <> 0 THEN token1_decimals
            END
        ) IS NOT NULL THEN ROUND(
            amount_out * p1.price,
            2
        )
    END AS amount_out_usd,
    CASE
        WHEN amount0_in_unadj <> 0 THEN token0_address
        WHEN amount1_in_unadj <> 0 THEN token1_address
    END AS token_address_in,
    CASE
        WHEN amount0_out_unadj <> 0 THEN token0_address
        WHEN amount1_out_unadj <> 0 THEN token1_address
    END AS token_address_out,
    CASE
        WHEN amount0_in_unadj <> 0 THEN token0_symbol
        WHEN amount1_in_unadj <> 0 THEN token1_symbol
    END AS symbol_in,
    CASE
        WHEN amount0_out_unadj <> 0 THEN token0_symbol
        WHEN amount1_out_unadj <> 0 THEN token1_symbol
    END AS symbol_out,
    COALESCE(
        lp_fee_unadj / pow(
            10,
            COALESCE(
                tokens.token_decimals,
                0
            )
        ),
        lp_fee_unadj,
        0
    ) AS lp_fee,
    CASE
        WHEN tokens.token_decimals IS NOT NULL THEN ROUND(
            lp_fee * p2.price,
            2
        )
    END AS lp_fee_usd,
    tokens.token_symbol AS lp_fee_symbol,
    CASE
        WHEN fee_currency IS NULL
        AND lp_fee <> 0
        AND pool_address <> '0xce9accfbb25eddce91845c3a7c3d1613d1d7081f' THEN token_address_in
        ELSE fee_currency
    END AS lp_fee_token_address,
    _log_id,
    COALESCE (
        swaps_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS fact_blocks_id,
    GREATEST(
        COALESCE(
            base.inserted_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            p0.inserted_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            p1.inserted_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            p2.inserted_timestamp,
            '2000-01-01'
        )
    ) AS inserted_timestamp,
    GREATEST(
        COALESCE(
            base.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            p0.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            p1.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            p2.modified_timestamp,
            '2000-01-01'
        )
    ) AS modified_timestamp
FROM
    {{ ref('silver__velodrome_swaps') }}
    base
    INNER JOIN velo_pools pools
    ON LOWER(
        base.contract_address
    ) = LOWER(
        pools.pool_address
    )
    LEFT JOIN tokens
    ON tokens.token_address = fee_currency
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p0
    ON p0.hour = DATE_TRUNC(
        'hour',
        block_timestamp
    )
    AND p0.token_address = CASE
        WHEN amount0_in_unadj <> 0 THEN token0_address
        WHEN amount1_in_unadj <> 0 THEN token1_address
    END
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p1
    ON p1.hour = DATE_TRUNC(
        'hour',
        block_timestamp
    )
    AND p1.token_address = CASE
        WHEN amount0_out_unadj <> 0 THEN token0_address
        WHEN amount1_out_unadj <> 0 THEN token1_address
    END
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p2
    ON p2.hour = DATE_TRUNC(
        'hour',
        block_timestamp
    )
    AND p2.token_address = fee_currency
