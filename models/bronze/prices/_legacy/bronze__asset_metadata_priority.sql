{{ config (
    materialized = 'view'
) }}

SELECT
    token_address,
    symbol,
    provider,
    id,
    _inserted_timestamp
FROM
    {{ source(
        'silver_crosschain',
        'asset_metadata_priority'
    ) }}
WHERE
    blockchain = 'optimism'
