{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['velodrome']
) }}

WITH lp_actions AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CASE
            WHEN topics [0] :: STRING IN(
                '0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496',
                '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f'
            ) THEN CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))
        END AS sender_address,
        CASE
            WHEN topics [0] :: STRING IN(
                '0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496',
                '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f'
            ) THEN PUBLIC.udf_hex_to_int(
                segmented_data [0] :: STRING
            ) :: FLOAT
        END AS amount0_unadj,
        CASE
            WHEN topics [0] :: STRING IN(
                '0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496',
                '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f'
            ) THEN PUBLIC.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) :: FLOAT
        END AS amount1_unadj,
        CASE
            WHEN topics [0] :: STRING IN(
                '0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496',
                '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f'
            ) THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))
        END AS to_address,
        CASE
            WHEN topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN (
                PUBLIC.udf_hex_to_int(
                    segmented_data [0] :: STRING
                ) :: FLOAT / pow(
                    10,
                    18
                )
            )
        END AS lp_token_amount,
        CASE
            WHEN topics [0] :: STRING = '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f' THEN 'deposit'
            WHEN topics [0] :: STRING = '0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496' THEN 'withdraw'
        END AS lp_action,
        CASE
            WHEN topics [0] :: STRING = '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f' THEN 'mint'
            WHEN topics [0] :: STRING = '0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496' THEN 'burn'
        END AS lp_token_action,
        topics [0] :: STRING AS function_type,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        (
            topics [0] :: STRING = '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f' -- deposits
            OR (
                topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' -- lp mints
                AND topics [1] :: STRING = '0x0000000000000000000000000000000000000000000000000000000000000000'
            )
            OR topics [0] :: STRING = '0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496' -- withdrawls
            OR (
                topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' -- lp burns
                AND topics [2] :: STRING = '0x0000000000000000000000000000000000000000000000000000000000000000'
            )
        )
        AND tx_status = 'SUCCESS'
        AND event_removed = 'false'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
velo_pools AS (
    SELECT
        pool_address,
        pool_name,
        pool_type,
        token0_symbol,
        token1_symbol,
        token0_address,
        token1_address,
        token0_decimals,
        token1_decimals
    FROM
        {{ ref('silver__velodrome_pools') }}
),
lp_tokens_actions AS (
    SELECT
        tx_hash,
        contract_address,
        lp_token_amount
    FROM
        lp_actions
    WHERE
        function_type = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
),
token_prices AS (
    SELECT
        HOUR,
        token_address,
        price
    FROM
        {{ ref('silver__prices') }}
    WHERE
        HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                lp_actions
        )
),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        A.tx_hash AS tx_hash,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        A.contract_address AS contract_address,
        sender_address,
        amount0_unadj,
        amount1_unadj,
        lp_action,
        lp_token_action,
        function_type,
        _log_id,
        _inserted_timestamp,
        pool_address,
        pool_name,
        pool_type,
        token0_symbol,
        token1_symbol,
        token0_address,
        token1_address,
        token0_decimals,
        token1_decimals,
        CASE
            WHEN token0_decimals IS NOT NULL THEN amount0_unadj / pow(
                10,
                token0_decimals
            )
            ELSE amount0_unadj
        END AS token_0_amount,
        CASE
            WHEN token1_decimals IS NOT NULL THEN amount1_unadj / pow(
                10,
                token1_decimals
            )
            ELSE amount1_unadj
        END AS token_1_amount,
        CASE
            WHEN token0_decimals IS NOT NULL THEN ROUND(
                token_0_amount * p0.price,
                2
            )
            ELSE NULL
        END AS token_0_amount_usd,
        CASE
            WHEN token1_decimals IS NOT NULL THEN ROUND(
                token_1_amount * p1.price,
                2
            )
            ELSE NULL
        END AS token_1_amount_usd,
        b.lp_token_amount AS lp_token_amount,
        token_1_amount_usd + token_0_amount_usd AS lp_token_amount_usd
    FROM
        lp_actions A
        INNER JOIN velo_pools
        ON LOWER(
            A.contract_address
        ) = LOWER(
            velo_pools.pool_address
        )
        LEFT JOIN token_prices AS p0
        ON p0.hour = DATE_TRUNC(
            'hour',
            block_timestamp
        )
        AND token0_address = p0.token_address
        LEFT JOIN token_prices AS p1
        ON p1.hour = DATE_TRUNC(
            'hour',
            block_timestamp
        )
        AND token1_address = p1.token_address
        LEFT JOIN lp_tokens_actions b
        ON A.tx_hash = b.tx_hash
        AND A.contract_address = b.contract_address
    WHERE
        function_type IN(
            '0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496',
            '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f'
        )
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    pool_address,
    pool_name,
    pool_type,
    sender_address,
    lp_action,
    token0_symbol,
    token1_symbol,
    token_0_amount AS token0_amount,
    token_1_amount AS token1_amount,
    token_0_amount_usd AS token0_amount_usd,
    token_1_amount_usd AS token1_amount_usd,
    token0_address,
    token1_address,
    lp_token_action,
    lp_token_amount,
    lp_token_amount_usd,
    _log_id,
    _inserted_timestamp
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC) = 1)
