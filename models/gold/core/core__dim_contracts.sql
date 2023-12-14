{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    c0.created_contract_address AS address,
    c1.token_symbol AS symbol,
    c1.token_name AS NAME,
    c1.token_decimals AS decimals,
    c0.block_number AS created_block_number,
    c0.block_timestamp AS created_block_timestamp,
    c0.tx_hash AS created_tx_hash,
    c0.creator_address AS creator_address,
    COALESCE (
        c0.created_contracts_id,
        {{ dbt_utils.generate_surrogate_key(
            ['c0.created_contract_address']
        ) }}
    ) AS dim_contracts_id,
    GREATEST(COALESCE(c0.inserted_timestamp, '2000-01-01'), COALESCE(c1.inserted_timestamp, '2000-01-01')) AS inserted_timestamp,
    GREATEST(COALESCE(c0.modified_timestamp, '2000-01-01'), COALESCE(c1.modified_timestamp, '2000-01-01')) AS modified_timestamp
FROM
    {{ ref('silver__created_contracts') }}
    c0
    LEFT JOIN {{ ref('silver__contracts') }}
    c1
    ON LOWER(
        c0.created_contract_address
    ) = LOWER(
        c1.contract_address
    )
UNION
SELECT
    contract_address AS address,
    token_symbol AS symbol,
    token_name AS NAME,
    token_decimals AS decimals,
    0 AS created_block_number,
    '1970-01-01 00:00:00' :: TIMESTAMP AS created_block_timestamp,
    'GENESIS' AS created_tx_hash,
    'GENESIS' AS creator_address,
    COALESCE (
        ovm1_contracts_id,
        {{ dbt_utils.generate_surrogate_key(
            ['contract_address']
        ) }}
    ) AS dim_contracts_id,
    GREATEST(COALESCE(inserted_timestamp, '2000-01-01'), COALESCE(inserted_timestamp, '2000-01-01')) AS inserted_timestamp,
    GREATEST(COALESCE(modified_timestamp, '2000-01-01'), COALESCE(modified_timestamp, '2000-01-01')) AS modified_timestamp
FROM
    {{ ref ('silver__ovm1_contracts') }}
