{{ config(
    materialized = 'incremental',
    unique_key = ['token_address', 'hour'],
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['non_realtime']
) }}

SELECT
    HOUR,
    token_address,
    price,
    is_imputed,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['token_address', 'hour']
    ) }} AS hourly_prices_priority_eth_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze__hourly_prices_priority_eth') }}
WHERE
    1 = 1

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
