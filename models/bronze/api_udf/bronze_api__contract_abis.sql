{{ config(
    materialized = 'incremental',
    unique_key = "contract_address",
    full_refresh = false
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
    100
)
SELECT
    contract_address,
    ethereum.streamline.udf_api(
        'GET',
        CONCAT(
            'https://api-optimistic.etherscan.io/api?module=contract&action=getabi&address=',
            contract_address,
            '&apikey=',
            api_key
        ),{},{}
    ) AS abi_data,
    SYSDATE() AS _inserted_timestamp
FROM
    base
    LEFT JOIN api_keys
    ON 1 = 1
WHERE
    EXISTS (
        SELECT
            1
        FROM
            base
        LIMIT
            1
    )
