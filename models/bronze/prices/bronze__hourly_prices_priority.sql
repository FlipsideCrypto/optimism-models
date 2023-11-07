{{ config (
    materialized = 'view'
) }}

SELECT
    HOUR,
    token_address,
    price,
    is_imputed,
    _inserted_timestamp
FROM
    {{ source(
        'silver_crosschain',
        'token_prices_priority_hourly'
    ) }}
WHERE
    blockchain = 'optimism'
