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
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_index,
    staking_action_type,
    lp_token_amount,
    lp_provider_address,
    gauge_address,
    base.pool_address,
    pool_name,
    pool_type,
    token0_symbol,
    token1_symbol,
    token0_address,
    token1_address,
    COALESCE (
        locks_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS ez_staking_actions_id,
    GREATEST(
        COALESCE(
            base.inserted_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            pools.inserted_timestamp,
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
        )
    ) AS modified_timestamp
FROM
    {{ ref('silver__velodrome_staking_actions') }}
    base
    INNER JOIN {{ ref('silver__velodrome_pools') }}
    pools
    ON base.pool_address = pools.pool_address
