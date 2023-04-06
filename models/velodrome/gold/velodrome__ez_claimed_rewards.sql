{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['velodrome'],
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
    contract_address,
    event_index,
    reward_type,
    token_id,
    CASE
        WHEN reward_type = 'venft_distribution' THEN claimed_amount
        ELSE COALESCE(
            claimed_amount / pow(
                10,
                token_decimals
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
        AND token_decimals IS NOT NULL THEN ROUND(
            (
                claimed_amount / pow(
                    10,
                    token_decimals
                )
            ) * prices.price,
            2
        )
        ELSE NULL
    END AS claimed_amount_usd,
    COALESCE(
        token_symbol,
        C.symbol
    ) AS token_symbol,
    base.token_address AS token_address,
    claim_epoch,
    max_epoch
FROM
    {{ ref('silver__velodrome_claimed_rewards') }}
    base
    LEFT JOIN tokens
    ON tokens.token_address = base.token_address
    LEFT JOIN {{ ref('core__fact_hourly_token_prices') }}
    prices
    ON HOUR = DATE_TRUNC(
        'hour',
        block_timestamp
    )
    AND prices.token_address = base.token_address
    LEFT JOIN {{ ref('core__dim_contracts') }} C
    ON C.address = base.token_address
