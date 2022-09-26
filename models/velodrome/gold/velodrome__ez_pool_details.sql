{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['velodrome']
) }}

SELECT
    pool_address,
    pool_name,
    pool_type,
    token0_symbol,
    token1_symbol,
    token0_address,
    token1_address,
    token0_decimals,
    token1_decimals,
    created_timestamp,
    created_block,
    created_hash,
    _inserted_timestamp
FROM
    {{ ref('silver__velodrome_pools') }}
