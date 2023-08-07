{{ config(
    materialized = 'incremental',
    unique_key = "contract_address",
    full_refresh = false,
    tags = ['non_realtime']
) }}

WITH api_keys AS (

    SELECT
        api_key
    FROM
        {{ source(
            'silver_crosschain',
            'apis_keys'
        ) }}
    WHERE
        api_name = 'op-etherscan'
),
base AS (
    SELECT
        contract_address
    FROM
        {{ ref('silver__relevant_abi_contracts') }}

{% if is_incremental() %}
EXCEPT
SELECT
    contract_address
FROM
    {{ this }}
WHERE
    abi_data :data :result :: STRING <> 'Max rate limit reached'
{% endif %}
LIMIT
    50
), all_contracts AS (
    SELECT
        contract_address
    FROM
        base
    UNION
    SELECT
        contract_address
    FROM
        (
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
                -- this has to be a source or else we get a circular dependency
                v USING (contract_address)
            WHERE
                l.block_timestamp >= CURRENT_DATE - INTERVAL '30 days' -- recent activity
                AND v.contract_address IS NULL -- no verified abi
                AND l.contract_address NOT IN (
                    SELECT
                        contract_address
                    FROM
                        {{ this }}
                    WHERE
                        _inserted_timestamp >= CURRENT_DATE - INTERVAL '30 days' -- this wont let us retry the same contract within 30 days
                        AND abi_data :data :result :: STRING <> 'Max rate limit reached' -- exclude contracts that we were rate limited on
                )
            GROUP BY
                contract_address
            ORDER BY
                events DESC
            LIMIT
                50
        )
), row_nos AS (
    SELECT
        contract_address,
        ROW_NUMBER() over (
            ORDER BY
                contract_address
        ) AS row_no,
        api_key
    FROM
        all_contracts
        JOIN api_keys
        ON 1 = 1
),
batched AS ({% for item in range(101) %}
SELECT
    rn.contract_address, ethereum.streamline.udf_api('GET', CONCAT('https://api-optimistic.etherscan.io/api?module=contract&action=getabi&address=', rn.contract_address, '&apikey=', api_key),{ 'User-Agent': 'FlipsideStreamline' },{}) AS abi_data, SYSDATE() AS _inserted_timestamp
FROM
    row_nos rn
WHERE
    row_no = {{ item }}

    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %})
SELECT
    contract_address,
    abi_data,
    _inserted_timestamp
FROM
    batched
