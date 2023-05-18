{{ config(
    materialized = 'incremental',
    unique_key = "contract_address"
) }}

WITH base AS (

    SELECT
        contract_address,
        PARSE_JSON(
            abi_data :data :result
        ) AS DATA,
        _inserted_timestamp
    FROM
        {{ ref('bronze_api__contract_abis') }}
    WHERE
        abi_data :data :message :: STRING = 'OK'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        COALESCE(
            MAX(
                _inserted_timestamp
            ),
            '1970-01-01'
        )
    FROM
        {{ this }}
)
{% endif %}
),
opscan_abis AS (
    SELECT
        contract_address,
        DATA,
        _inserted_timestamp,
        'opscan' AS abi_source
    FROM
        base
),
user_abis AS (
    SELECT
        contract_address,
        abi,
        discord_username,
        _inserted_timestamp,
        'user' AS abi_source,
        abi_hash
    FROM
        {{ ref('silver__user_verified_abis') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(
                MAX(
                    _inserted_timestamp
                ),
                '1970-01-01'
            )
        FROM
            {{ this }}
        WHERE
            abi_source = 'user'
    )
    AND contract_address NOT IN (
        SELECT
            contract_address
        FROM
            {{ this }}
    )
{% endif %}
),
all_abis AS (
    SELECT
        contract_address,
        DATA,
        _inserted_timestamp,
        abi_source,
        NULL AS discord_username,
        SHA2(DATA) AS abi_hash
    FROM
        opscan_abis
    UNION
    SELECT
        contract_address,
        PARSE_JSON(abi) AS DATA,
        _inserted_timestamp,
        'user' AS abi_source,
        discord_username,
        abi_hash
    FROM
        user_abis
)
SELECT
    contract_address,
    DATA,
    _inserted_timestamp,
    abi_source,
    discord_username,
    abi_hash
FROM
    all_abis qualify(ROW_NUMBER() over(PARTITION BY contract_address
ORDER BY
    _INSERTED_TIMESTAMP DESC)) = 1
