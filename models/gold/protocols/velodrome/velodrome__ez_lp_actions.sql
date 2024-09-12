{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'VELODROME',
                'PURPOSE': 'DEFI, DEX'
            }
        }
    }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    pool_address,
    pool_name,
    pool_type,
    sender_address,
    lp_action,
    token0_symbol,
    token1_symbol,
    CASE
        WHEN token0_decimals IS NOT NULL THEN amount0_unadj / pow(
            10,
            token0_decimals
        )
        ELSE amount0_unadj
    END AS token0_amount,
    CASE
        WHEN token1_decimals IS NOT NULL THEN amount1_unadj / pow(
            10,
            token1_decimals
        )
        ELSE amount1_unadj
    END AS token1_amount,
    CASE
        WHEN token0_decimals IS NOT NULL THEN ROUND(
            token0_amount * p0.price,
            2
        )
        ELSE NULL
    END AS token0_amount_usd,
    CASE
        WHEN token1_decimals IS NOT NULL THEN ROUND(
            token1_amount * p1.price,
            2
        )
        ELSE NULL
    END AS token1_amount_usd,
    token0_address,
    token1_address,
    lp_token_action,
    lp_token_amount,
    token0_amount_usd + token1_amount_usd AS lp_token_amount_usd,
    COALESCE (
        lp_actions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS ez_lp_actions_id,
    GREATEST(
        COALESCE(
            base.inserted_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            pools.inserted_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            p0.inserted_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            p1.inserted_timestamp,
            '2000-01-01'
        )
    ) AS inserted_timestamp,
    GREATEST(
        COALESCE(
            base.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            pools.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            p0.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            p1.modified_timestamp,
            '2000-01-01'
        )
    ) AS modified_timestamp
FROM
    {{ ref('silver__velodrome_LP_actions') }}
    base
    INNER JOIN {{ ref('silver__velodrome_pools') }}
    pools
    ON LOWER(
        base.contract_address
    ) = LOWER(
        pools.pool_address
    )
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p0
    ON p0.hour = DATE_TRUNC(
        'hour',
        block_timestamp
    )
    AND token0_address = p0.token_address
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p1
    ON p1.hour = DATE_TRUNC(
        'hour',
        block_timestamp
    )
    AND token1_address = p1.token_address
