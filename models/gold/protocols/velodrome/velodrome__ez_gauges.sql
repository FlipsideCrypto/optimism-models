{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['velodrome'],
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
    block_timestamp,
    block_number,
    tx_hash,
    event_index,
    contract_address,
    gauge_address,
    external_bribe_address,
    internal_bribe_address,
    creator_address,
    A.pool_address AS pool_address,
    pool_name,
    pool_type,
    token0_symbol,
    token1_symbol,
    token0_address,
    token1_address
FROM
    {{ ref('silver__velodrome_gauges') }} A
    LEFT JOIN {{ ref('silver__velodrome_pools') }}
    b
    ON A.pool_address = b.pool_address
