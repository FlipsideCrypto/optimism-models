{{ config(
    materialized = 'incremental',
    unique_key = "contract_address",
    full_refresh = false,
    tags = ['curated']
) }}

WITH base AS (

    SELECT
        contract_address
    FROM
        {{ ref('silver__relevant_contracts') }}
    WHERE
        total_interaction_count >= 100

{% if is_incremental() %}
and contract_address not in (
SELECT
    contract_address
FROM
    {{ this }}
    WHERE
        abi_data :data :result :: STRING <> 'Max rate limit reached'
)
{% endif %}
order by total_interaction_count desc
LIMIT
    400
), all_contracts AS (
    SELECT
        contract_address
    FROM
        base
    UNION
    SELECT
        contract_address
    FROM
        {{ ref('_retry_abis') }}
),
row_nos AS (
    SELECT
        contract_address,
        ROW_NUMBER() over (
            ORDER BY
                contract_address
        ) AS row_no
    FROM
        all_contracts
),
batched AS ({% for item in range(501) %}
SELECT
    rn.contract_address, live.udf_api('GET', CONCAT('https://api-optimistic.etherscan.io/api?module=contract&action=getabi&address=', rn.contract_address, '&apikey={key}'),{ 'User-Agent': 'FlipsideStreamline' },{}, 'Vault/prod/block_explorers/op_etherscan') AS abi_data, SYSDATE() AS _inserted_timestamp
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
