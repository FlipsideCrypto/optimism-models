{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['velodrome']
) }}

WITH base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CASE
            WHEN topics [0] :: STRING = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822' THEN CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))
        END AS sender_address,
        CASE
            WHEN topics [0] :: STRING = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822' THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))
        END AS to_address,
        CASE
            WHEN topics [0] :: STRING = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822' THEN PUBLIC.udf_hex_to_int(
                segmented_data [0] :: STRING
            ) :: FLOAT
        END AS amount0_in_adj,
        CASE
            WHEN topics [0] :: STRING = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822' THEN PUBLIC.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) :: FLOAT
        END AS amount1_in_adj,
        CASE
            WHEN topics [0] :: STRING = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822' THEN PUBLIC.udf_hex_to_int(
                segmented_data [2] :: STRING
            ) :: FLOAT
        END AS amount0_out_adj,
        CASE
            WHEN topics [0] :: STRING = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822' THEN PUBLIC.udf_hex_to_int(
                segmented_data [3] :: STRING
            ) :: FLOAT
        END AS amount1_out_adj,
        CASE
            WHEN topics [0] :: STRING = '0x112c256902bf554b6ed882d2936687aaeb4225e8cd5b51303c90ca6cf43a8602' THEN PUBLIC.udf_hex_to_int(
                segmented_data [0] :: STRING
            ) :: FLOAT
        END AS fees0_adj,
        CASE
            WHEN topics [0] :: STRING = '0x112c256902bf554b6ed882d2936687aaeb4225e8cd5b51303c90ca6cf43a8602' THEN PUBLIC.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) :: FLOAT
        END AS fees1_adj,
        CASE
            WHEN topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN PUBLIC.udf_hex_to_int(
                segmented_data [0] :: STRING
            ) :: FLOAT
        END AS transfer_amount,
        _log_id,
        _inserted_timestamp,
        event_index,
        'velodrome' AS platform,
        topics [0] :: STRING AS function_type
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING IN (
            '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822',
            '0x112c256902bf554b6ed882d2936687aaeb4225e8cd5b51303c90ca6cf43a8602',
            '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
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
tokens AS (
    SELECT
        DISTINCT token0_address AS token_address,
        token0_decimals AS token_decimals,
        token0_symbol AS token_symbol
    FROM
        velo_pools
    UNION
    SELECT
        DISTINCT token1_address AS token_address,
        token1_decimals AS token_decimals,
        token1_symbol AS token_symbol
    FROM
        velo_pools
),
transfers AS (
    SELECT
        tx_hash,
        contract_address AS fee_currency,
        transfer_amount AS fee_amount,
        token_decimals AS fee_decimals,
        token_symbol AS fee_symbol
    FROM
        base
        LEFT JOIN tokens
        ON token_address = contract_address
    WHERE
        function_type = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
),
lp_fees AS (
    SELECT
        A.tx_hash AS tx_hash,
        A.contract_address AS contract_address,
        fees0_adj,
        fees1_adj,
        CASE
            WHEN fees0_adj = 0 THEN fees1_adj :: FLOAT
            WHEN fees1_adj = 0 THEN fees0_adj :: FLOAT
        END AS fees_adj,
        fee_currency,
        fee_decimals,
        fee_symbol,
        ROW_NUMBER() over (
            PARTITION BY A.tx_hash,
            contract_address
            ORDER BY
                event_index ASC
        ) AS agg_id
    FROM
        base A
        LEFT JOIN transfers b
        ON A.tx_hash = b.tx_hash
        AND (
            CASE
                WHEN fees0_adj = 0 THEN fees1_adj :: FLOAT
                WHEN fees1_adj = 0 THEN fees0_adj :: FLOAT
            END
        ) = b.fee_amount
    WHERE
        function_type = '0x112c256902bf554b6ed882d2936687aaeb4225e8cd5b51303c90ca6cf43a8602'
),
swaps AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        sender_address,
        to_address,
        amount0_in_adj,
        amount1_in_adj,
        amount0_out_adj,
        amount1_out_adj,
        _log_id,
        _inserted_timestamp,
        event_index,
        platform,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            contract_address
            ORDER BY
                event_index ASC
        ) AS agg_id
    FROM
        base
    WHERE
        function_type = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822'
),
combine_meta AS (
    SELECT
        block_number,
        block_timestamp,
        b.tx_hash AS tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        b.contract_address AS contract_address,
        sender_address,
        to_address,
        amount0_in_adj,
        amount0_out_adj,
        amount1_in_adj,
        amount1_out_adj,
        _log_id,
        _inserted_timestamp,
        event_index,
        platform,
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
            WHEN amount0_in_adj <> 0 THEN token0_decimals
            WHEN amount1_in_adj <> 0 THEN token1_decimals
        END AS decimals_in,
        CASE
            WHEN amount0_out_adj <> 0 THEN token0_decimals
            WHEN amount1_out_adj <> 0 THEN token1_decimals
        END AS decimals_out,
        CASE
            WHEN amount0_in_adj <> 0 THEN amount0_in_adj
            WHEN amount1_in_adj <> 0 THEN amount1_in_adj
        END AS amount_in_adj,
        CASE
            WHEN amount0_out_adj <> 0 THEN amount0_out_adj
            WHEN amount1_out_adj <> 0 THEN amount1_out_adj
        END AS amount_out_adj,
        CASE
            WHEN decimals_in IS NOT NULL THEN amount_in_adj / pow(
                10,
                decimals_in
            )
            ELSE amount_in_adj
        END AS amount_in,
        CASE
            WHEN decimals_out IS NOT NULL THEN amount_out_adj / pow(
                10,
                decimals_out
            )
            ELSE amount_out_adj
        END AS amount_out,
        CASE
            WHEN amount0_in_adj <> 0 THEN token0_address
            WHEN amount1_in_adj <> 0 THEN token1_address
        END AS token_address_in,
        CASE
            WHEN amount0_out_adj <> 0 THEN token0_address
            WHEN amount1_out_adj <> 0 THEN token1_address
        END AS token_address_out,
        CASE
            WHEN amount0_in_adj <> 0 THEN token0_symbol
            WHEN amount1_in_adj <> 0 THEN token1_symbol
        END AS symbol_in,
        CASE
            WHEN amount0_out_adj <> 0 THEN token0_symbol
            WHEN amount1_out_adj <> 0 THEN token1_symbol
        END AS symbol_out,
        CASE
            WHEN fee_decimals IS NOT NULL THEN fees_adj / pow(
                10,
                fee_decimals
            )
            ELSE fees_adj
        END AS lp_fee,
        fee_currency,
        fee_decimals,
        fee_symbol
    FROM
        swaps b
        INNER JOIN velo_pools
        ON b.contract_address = pool_address
        LEFT JOIN lp_fees l
        ON b.contract_address = l.contract_address
        AND b.tx_hash = l.tx_hash
        AND b.agg_id = l.agg_id
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
                combine_meta
        )
)
SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    contract_address,
    pool_address,
    pool_name,
    amount_in,
    CASE
        WHEN decimals_in IS NOT NULL THEN ROUND(
            amount_in * p0.price,
            2
        )
        ELSE NULL
    END AS amount_in_usd,
    amount_out,
    CASE
        WHEN decimals_out IS NOT NULL THEN ROUND(
            amount_out * p1.price,
            2
        )
        ELSE NULL
    END AS amount_out_usd,
    sender_address,
    to_address,
    event_index,
    _log_id,
    platform,
    _inserted_timestamp,
    token_address_in,
    token_address_out,
    symbol_in,
    symbol_out,
    decimals_in,
    decimals_out,
    token0_decimals,
    token1_decimals,
    token0_symbol,
    token1_symbol,
    lp_fee,
    CASE
        WHEN fee_decimals IS NOT NULL THEN ROUND(
            lp_fee * p3.price,
            2
        )
        ELSE NULL
    END AS lp_fee_usd,
    fee_symbol AS lp_fee_symbol,
    fee_currency AS lp_fee_token_address
FROM
    combine_meta
    LEFT JOIN token_prices AS p0
    ON p0.hour = DATE_TRUNC(
        'hour',
        block_timestamp
    )
    AND token_address_in = p0.token_address
    LEFT JOIN token_prices AS p1
    ON p1.hour = DATE_TRUNC(
        'hour',
        block_timestamp
    )
    AND token_address_out = p1.token_address
    LEFT JOIN token_prices AS p3
    ON p3.hour = DATE_TRUNC(
        'hour',
        block_timestamp
    )
    AND fee_currency = p3.token_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC) = 1)
