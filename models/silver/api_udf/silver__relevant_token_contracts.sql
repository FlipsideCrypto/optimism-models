{{ config(
    materialized = 'table',
    unique_key = "contract_address"
) }}

SELECT
    contract_address,
    'optimism' AS blockchain,
    COUNT(*) AS transfers,
    MIN(block_number) AS created_block
FROM
    {{ ref('silver__logs') }}
GROUP BY
    1,
    2
HAVING
    COUNT(*) > 25
