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
    blockchain = 'ethereum'
    AND token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
