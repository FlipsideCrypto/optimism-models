{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    LOWER(address) :: STRING AS address,
    symbol :: STRING AS symbol,
    decimals :: INTEGER AS decimals
FROM
    {{ ref('silver__contracts_backfill') }}
