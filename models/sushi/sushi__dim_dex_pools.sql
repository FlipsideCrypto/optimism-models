{{ config(
    materialized = 'table',
    enabled = false,
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'SUSHI',
                'PURPOSE': 'DEFI, DEX'
            }
        }
    }
) }}

    SELECT
        pool_address,
        pool as pool_name,
        fee_tier,
        TWAP,
        lower(token0_address) as token0_address,
        token0_name as token0_symbol,
        lower(token1_address) as token1_address,
        token1_name as token1_symbol,
        token0_decimal as token0_decimals,
        token1_decimal as token1_decimals 
    FROM
         {{ source(
            'optimism_pools',
            'SUSHI_DIM_DEX_POOLS'
        ) }} 