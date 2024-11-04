{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'created_block',
    tags = ['stale']
) }}

SELECT
    created_timestamp,
    created_block,
    created_hash,
    l.contract_address AS deployer_address,
    pool_address,
    pool_name,
    pool_type,
    pool_symbol,
    pool_decimals,
    token0_symbol,
    token1_symbol,
    token0_address,
    token1_address,
    token0_decimals,
    token1_decimals,
    l._log_id,
    p._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['pool_address']
    ) }} AS pool_details_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__velodrome_pools') }}
    p
    LEFT JOIN {{ ref('core__fact_event_logs') }}
    l
    ON p.created_hash = l.tx_hash
    AND p.created_block = l.block_number

{% if is_incremental() %}
WHERE
    p._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
    AND l._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY pool_address
ORDER BY
    p._inserted_timestamp DESC)) = 1
