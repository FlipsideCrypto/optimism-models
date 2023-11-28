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
    _inserted_timestamp,
    COALESCE (
        pools_id,
        {{ dbt_utils.generate_surrogate_key(
            ['pool_address']
        ) }}
    ) AS ez_pool_details_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__velodrome_pools') }}
