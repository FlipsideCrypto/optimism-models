{{ config(
    materialized = 'incremental',
    unique_key = "block_number"
) }}

SELECT
    block_number,
    MIN(_inserted_timestamp) AS _inserted_timestamp,
    COUNT(*) AS tx_count
FROM
    {{ ref('silver__transactions2') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% endif %}
GROUP BY
    block_number