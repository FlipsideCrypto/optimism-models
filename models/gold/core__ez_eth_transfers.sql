{{ config(
    materialized = 'view'
) }}

WITH eth_base AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address,
        to_address,
        eth_value,
        identifier,
        _call_id,
        ingested_at,
        input
    FROM
        {{ ref('silver__traces') }}
    WHERE
        eth_value > 0
        AND tx_status = 'SUCCESS'
        AND gas_used IS NOT NULL
),
eth_price AS (
    SELECT
        HOUR,
        price AS eth_price
    FROM
        {{ ref('silver__prices_eth') }}
)
SELECT
    A.tx_hash AS tx_hash,
    A.block_number AS block_number,
    A.block_timestamp AS block_timestamp,
    A.identifier AS identifier,
    tx.from_address AS origin_from_address,
    tx.to_address AS origin_to_address,
    tx.origin_function_signature AS origin_function_signature,
    A.from_address AS eth_from_address,
    A.to_address AS eth_to_address,
    A.eth_value AS amount,
    ROUND(
        A.eth_value * eth_price,
        2
    ) AS amount_usd
FROM
    eth_base A
    LEFT JOIN eth_price
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = HOUR
    JOIN {{ ref('silver__transactions') }}
    tx
    ON A.tx_hash = tx.tx_hash