{{ config (
    materialized = "ephemeral"
) }}

WITH retry AS (

    SELECT
        contract_address,
        MAX(block_number) AS block_number,
        COUNT(*) AS events
    FROM
        {{ ref("silver__logs") }}
        l
        LEFT JOIN {{ source(
            'optimism_silver',
            'verified_abis'
        ) }}
        v USING (contract_address)
    WHERE
        l.block_timestamp >= CURRENT_DATE - INTERVAL '30 days' -- recent activity
        AND v.contract_address IS NULL -- no verified abi
        AND l.contract_address NOT IN (
            SELECT
                contract_address
            FROM
                {{ source(
                    'optimism_bronze_api',
                    'contract_abis'
                ) }}
            WHERE
                _inserted_timestamp >= CURRENT_DATE - INTERVAL '30 days' -- this won't let us retry the same contract within 30 days
                AND abi_data :data :result :: STRING <> 'Max rate limit reached'
        )
    GROUP BY
        contract_address
    ORDER BY
        events DESC
    LIMIT
        25
), FINAL AS (
    SELECT
        proxy_address AS contract_address,
        start_block AS block_number
    FROM
        {{ ref("silver__proxies") }}
        p
        JOIN retry r USING (contract_address)
        LEFT JOIN {{ source(
            'optimism_silver',
            'verified_abis'
        ) }}
        v
        ON v.contract_address = p.proxy_address
    WHERE
        v.contract_address IS NULL
        AND p.contract_address NOT IN (
            SELECT
                contract_address
            FROM
                {{ source(
                    'optimism_bronze_api',
                    'contract_abis'
                ) }}
            WHERE
                _inserted_timestamp >= CURRENT_DATE - INTERVAL '30 days' -- this won't let us retry the same contract within 30 days
                AND abi_data :data :result :: STRING <> 'Max rate limit reached'
        )
    UNION ALL
    SELECT
        contract_address,
        block_number
    FROM
        retry
)
SELECT
    *
FROM
    FINAL qualify ROW_NUMBER() over (
        PARTITION BY contract_address
        ORDER BY
            block_number DESC
    ) = 1
