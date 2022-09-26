{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['velodrome']
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
    token0_amount_usd + token1_amount_usd AS lp_token_amount_usd
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
    LEFT JOIN {{ ref('silver__prices') }}
    p0
    ON p0.hour = DATE_TRUNC(
        'hour',
        block_timestamp
    )
    AND token0_address = p0.token_address
    LEFT JOIN {{ ref('silver__prices') }}
    p1
    ON p1.hour = DATE_TRUNC(
        'hour',
        block_timestamp
    )
    AND token1_address = p1.token_address
