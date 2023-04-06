{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    HOUR,
    token_address,
    symbol,
    decimals,
    price,
    is_imputed
FROM
    {{ source(
        'crosschain',
        'ez_hourly_prices'
    ) }}
WHERE blockchain = 'optimism'
