{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'VELODROME',
    'PURPOSE': 'DEFI, DEX' } } }
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
        DISTINCT token_address,
        token_symbol,
        token_decimals
    FROM
        (
            SELECT
                token0_address AS token_address,
                token0_symbol AS token_symbol,
                token0_decimals AS token_decimals
            FROM
                velo_pools
            UNION
            SELECT
                token1_address AS token_address,
                token1_symbol AS token_symbol,
                token1_decimals AS token_decimals
            FROM
                velo_pools
        )
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    base.contract_address,
    event_index,
    reward_type,
    token_id,
    CASE
        WHEN reward_type = 'venft_distribution' THEN claimed_amount
        ELSE COALESCE(
            claimed_amount / pow(
                10,
                t.token_decimals
            ),
            claimed_amount
        )
    END AS claimed_amount,
    CASE
        WHEN reward_type = 'venft_distribution' THEN ROUND(
            claimed_amount * prices.price,
            2
        )
        WHEN reward_type <> 'venft_distribution'
        AND t.token_decimals IS NOT NULL THEN ROUND(
            (
                claimed_amount / pow(
                    10,
                    t.token_decimals
                )
            ) * prices.price,
            2
        )
        ELSE NULL
    END AS claimed_amount_usd,
    COALESCE(
        t.token_symbol,
        C.token_symbol
    ) AS token_symbol,
    base.token_address AS token_address,
    claim_epoch,
    max_epoch,
    COALESCE (
        claimed_rewards_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS ez_claimed_rewards_id,
    greatest(
    COALESCE(
        base.inserted_timestamp,
        '2000-01-01'
    ),
    COALESCE(
        prices.inserted_timestamp,
        '2000-01-01'
    ),
    COALESCE(
        C.inserted_timestamp,
        '2000-01-01'
    )
    ) AS inserted_timestamp,
    greatest(
    COALESCE(
        base.modified_timestamp,
        '2000-01-01'
    ),
    COALESCE(
        prices.modified_timestamp,
        '2000-01-01'
    ),
    COALESCE(
        C.modified_timestamp,
        '2000-01-01'
    )) AS modified_timestamp
FROM
    {{ ref('silver__velodrome_claimed_rewards') }}
    base
    LEFT JOIN tokens t 
    ON t.token_address = base.token_address
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    prices
    ON HOUR = DATE_TRUNC(
        'hour',
        block_timestamp
    )
    AND prices.token_address = base.token_address
    LEFT JOIN {{ ref('silver__contracts') }} C
    ON C.contract_address = base.token_address
