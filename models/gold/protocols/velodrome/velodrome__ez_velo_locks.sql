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
    provider_address,
    velo_action,
    unlock_date,
    token_id,
    velo_amount,
    ROUND(
        velo_amount * price,
        2
    ) AS velo_amount_usd,
    deposit_type,
    COALESCE (
        locks_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS ez_velo_locks_id,
    GREATEST(
        COALESCE(
            l.inserted_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            prices.inserted_timestamp,
            '2000-01-01'
        )
    ) AS inserted_timestamp,
    GREATEST(
        COALESCE(
            l.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            prices.modified_timestamp,
            '2000-01-01'
        )
    ) AS modified_timestamp
FROM
    {{ ref('silver__velodrome_locks') }} l 
    LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
    prices
    ON HOUR = DATE_TRUNC(
        'hour',
        block_timestamp
    )
WHERE
    symbol = 'VELO'
