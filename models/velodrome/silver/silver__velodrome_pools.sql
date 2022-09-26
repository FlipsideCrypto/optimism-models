{{ config(
    materialized = 'incremental',
    unique_key = 'pool_address',
    tags = ['velodrome']
) }}

WITH pool_backfill AS (

    SELECT
        LOWER(pool_address) AS pool_address,
        pool_name,
        pool_type,
        token0_symbol,
        token1_symbol,
        LOWER(token0_address) AS token0_address,
        LOWER(token1_address) AS token1_address,
        token0_decimals,
        token1_decimals,
        '1970-01-01' :: DATE AS _inserted_timestamp
    FROM
        {{ ref('silver__velo_pool_backfill') }}
),
token_backfill AS (
    SELECT
        symbol,
        op_token_address AS contract_address,
        decimals
    FROM
        {{ ref('silver__velo_tokens_backup') }}
),
contracts AS (
    SELECT
        address AS contract_address,
        symbol,
        decimals
    FROM
        {{ ref('core__dim_contracts') }}
),
new_pools AS (
    SELECT
        block_timestamp AS created_timestamp,
        block_number AS created_block,
        tx_hash AS created_hash,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS token0_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token1_address,
        CONCAT('0x', SUBSTR(DATA :: STRING, 91, 40)) AS pool_address,
        CASE
            WHEN SUBSTR(
                DATA,
                66,
                1
            ) = 1 THEN 'stable'
            ELSE 'volatile'
        END AS pool_type,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xc4805696c66d7cf352fc1d6bb633ad5ee82f6cb577c453024b6e0eb8306c6fc9' -- pair created
        AND contract_address = '0x25cbddb98b35ab1ff77413456b31ec81a6b6b746' -- velo depolyer

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE
    FROM
        {{ this }}
)
{% endif %}
),

{% if is_incremental() %}
missing_pools AS (
    SELECT
        created_timestamp,
        created_block,
        created_hash,
        token0_address,
        token1_address,
        pool_address,
        pool_type,
        _inserted_timestamp
    FROM
        {{ this }}
    WHERE
        pool_name IS NULL
        OR token0_decimals IS NULL
        OR token1_decimals IS NULL
),
{% endif %}

heal_pools AS (
    SELECT
        created_timestamp,
        created_block,
        created_hash,
        token0_address,
        token1_address,
        pool_address,
        pool_type,
        _inserted_timestamp
    FROM
        new_pools

{% if is_incremental() %}
UNION
SELECT
    created_timestamp,
    created_block,
    created_hash,
    token0_address,
    token1_address,
    pool_address,
    pool_type,
    _inserted_timestamp
FROM
    missing_pools
{% endif %}
),
add_meta AS (
    SELECT
        created_timestamp,
        created_block,
        created_hash,
        token0_address,
        COALESCE(
            tb0.symbol,
            c0.symbol
        ) AS token0_symbol,
        COALESCE(
            tb1.symbol,
            c1.symbol
        ) AS token1_symbol,
        COALESCE(
            tb0.decimals,
            c0.decimals
        ) AS token0_decimals,
        COALESCE(
            tb1.decimals,
            c1.decimals
        ) AS token1_decimals,
        token1_address,
        pool_address,
        pool_type,
        _inserted_timestamp
    FROM
        heal_pools
        LEFT JOIN token_backfill AS tb0
        ON token0_address = tb0.contract_address
        LEFT JOIN contracts AS c0
        ON token0_address = c0.contract_address
        LEFT JOIN token_backfill AS tb1
        ON token1_address = tb1.contract_address
        LEFT JOIN contracts AS c1
        ON token1_address = c1.contract_address
),
name_pools AS (
    SELECT
        created_timestamp,
        created_block,
        created_hash,
        token0_address,
        token0_symbol,
        token1_symbol,
        token0_decimals,
        token1_decimals,
        token1_address,
        pool_address,
        pool_type,
        _inserted_timestamp,
        CONCAT(
            LOWER(
                LEFT(
                    pool_type,
                    1
                )
            ),
            'AMM-',
            token0_symbol,
            '/',
            token1_symbol
        ) AS pool_name
    FROM
        add_meta
),
combine AS (
    SELECT
        pool_address,
        pool_name,
        pool_type,
        token0_symbol,
        token1_symbol,
        token0_address,
        token1_address,
        token0_decimals,
        token1_decimals,
        created_timestamp,
        created_block,
        created_hash,
        _inserted_timestamp
    FROM
        name_pools
    UNION ALL
    SELECT
        pool_address,
        pool_name,
        pool_type,
        token0_symbol,
        token1_symbol,
        token0_address,
        token1_address,
        token0_decimals,
        token1_decimals,
        NULL AS created_timestamp,
        NULL AS created_block,
        NULL AS created_hash,
        _inserted_timestamp
    FROM
        pool_backfill
)
SELECT
    LOWER(pool_address) AS pool_address,
    pool_name,
    pool_type,
    token0_symbol,
    token1_symbol,
    LOWER(token0_address) AS token0_address,
    LOWER(token1_address) AS token1_address,
    token0_decimals,
    token1_decimals,
    created_timestamp,
    created_block,
    created_hash,
    _inserted_timestamp
FROM
    combine qualify(ROW_NUMBER() over(PARTITION BY pool_address
ORDER BY
    _inserted_timestamp DESC) = 1)
