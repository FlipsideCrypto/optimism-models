{{ config(
    materialized = 'incremental',
    unique_key = 'pool_address',
    tags = ['non_realtime']
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
    p._inserted_timestamp
FROM
    {{ ref('silver__velodrome_pools') }}
    p
    LEFT JOIN {{ ref('silver__logs') }}
    l
    ON p.created_hash = l.tx_hash
    AND p.created_block = l.block_number

{% if is_incremental() %}
WHERE
    p._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) :: DATE - 1
        FROM
            {{ this }}
    )
    AND l._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) :: DATE - 1
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY pool_address
ORDER BY
    p._inserted_timestamp DESC)) = 1