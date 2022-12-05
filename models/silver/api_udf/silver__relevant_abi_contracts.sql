{{ config(
    materialized = 'table',
    unique_key = "contract_address"
) }}

WITH base AS (

    SELECT
        contract_address
    FROM
        {{ ref('silver__relevant_token_contracts') }}
),
proxies AS (
    SELECT
        tx_hash,
        block_number,
        contract_address,
        CONCAT('0x', SUBSTR(DATA, 27, 40)) AS proxy_address1,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS proxy_address2,
        CASE
            WHEN proxy_address1 = '0x' THEN proxy_address2
            ELSE proxy_address1
        END AS proxy_address,
        topics,
        DATA
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address IN (
            SELECT
                contract_address
            FROM
                base
        )
        AND topics [0] :: STRING = '0xbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b'
        AND tx_status = 'SUCCESS' qualify(ROW_NUMBER() over(PARTITION BY proxy_address
    ORDER BY
        block_number DESC)) = 1
)
SELECT
    contract_address
FROM
    base
UNION
SELECT
    proxy_address AS contract_address
FROM
    proxies
