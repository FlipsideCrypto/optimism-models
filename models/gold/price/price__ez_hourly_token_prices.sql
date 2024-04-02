{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    HOUR,
    token_address,
    id,
    symbol,
    decimals,
    price,
    blockchain,
    blockchain_name,
    blockchain_id,
    is_imputed,
    is_deprecated,
    complete_token_prices_id AS ez_hourly_token_prices_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__complete_token_prices') }}
