{{ config(
    materialized = 'incremental',
    unique_key = 'contract_address',
    tags = ['stale']
) }}

SELECT
    contract_address,
    token_name,
    token_decimals,
    token_symbol,
    _inserted_timestamp
FROM
    {{ ref('silver__contracts') }}
    c1
WHERE
    NOT EXISTS (
        SELECT
            1
        FROM
            {{ ref('silver__created_contracts') }}
            c0
        WHERE
            c1.contract_address = c0.created_contract_address
    )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
