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
        {{ ref('_retry_abis') }}
),
row_nos AS (
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
batched AS ({% for item in range(150) %}
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
