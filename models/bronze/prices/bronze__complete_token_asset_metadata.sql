{{ config (
    materialized = 'view'
) }}

SELECT
    token_address,
    id,
    symbol,
    NAME,
    decimals,
    blockchain,
    blockchain_name,
    blockchain_id,
    is_deprecated,
    provider,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_token_asset_metadata_id
FROM
    {{ source(
        'silver_crosschain',
        'complete_token_asset_metadata'
    ) }}
WHERE
    blockchain = 'optimism'