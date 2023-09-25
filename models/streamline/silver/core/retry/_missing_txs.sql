{{ config (
    materialized = "ephemeral"
) }}

WITH transactions AS (

    SELECT
        block_number,
        POSITION,
        LAG(
            POSITION,
            1
        ) over (
            PARTITION BY block_number
            ORDER BY
                POSITION ASC
        ) AS prev_POSITION
    FROM
        {{ ref("silver__transactions") }}
    WHERE
        block_timestamp >= DATEADD('hour', -48, SYSDATE()))
    SELECT
        DISTINCT block_number AS block_number
    FROM
        transactions
    WHERE
        POSITION - prev_POSITION <> 1
