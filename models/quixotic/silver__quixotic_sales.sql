{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base AS (

    SELECT
        block_number,
        block_timestamp,
        origin_to_address,
        origin_from_address,
        origin_function_signature,
        event_index,
        tx_hash,
        'sale' AS event_type,
        contract_address AS platform_address,
        'quixotic' AS platform_name,
        _inserted_timestamp,
        _log_id,
        topics [0] :: STRING AS function_type,
        CASE
            WHEN topics [0] :: STRING = '0x70ba0d31158674eea8365d0f7b9ac70e552cc28b8bb848664e4feb939c6578f8' THEN regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}')END AS segmented_data,
            CASE
                WHEN topics [0] :: STRING = '0x70ba0d31158674eea8365d0f7b9ac70e552cc28b8bb848664e4feb939c6578f8' THEN CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))
            END AS seller_address,
            CASE
                WHEN topics [0] :: STRING = '0x70ba0d31158674eea8365d0f7b9ac70e552cc28b8bb848664e4feb939c6578f8' THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))
            END AS nft_address,
            CASE
                WHEN topics [0] :: STRING = '0x70ba0d31158674eea8365d0f7b9ac70e552cc28b8bb848664e4feb939c6578f8' THEN ethereum.public.udf_hex_to_int(
                    topics [3] :: STRING
                )
            END AS tokenID,
            CASE
                WHEN topics [0] :: STRING = '0x70ba0d31158674eea8365d0f7b9ac70e552cc28b8bb848664e4feb939c6578f8' THEN CONCAT(
                    '0x',
                    SUBSTR(
                        segmented_data [0] :: STRING :: STRING,
                        25,
                        40
                    )
                )
            END AS buyer_address,
            CASE
                WHEN topics [0] :: STRING = '0x70ba0d31158674eea8365d0f7b9ac70e552cc28b8bb848664e4feb939c6578f8' THEN ethereum.public.udf_hex_to_int(
                    segmented_data [1] :: STRING
                ) / pow(
                    10,
                    18
                )
            END AS price
            FROM
                {{ ref('silver__logs') }}
            WHERE
                (
                    topics [0] :: STRING = '0x70ba0d31158674eea8365d0f7b9ac70e552cc28b8bb848664e4feb939c6578f8'
                    OR (
                        topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
                        AND contract_address = '0x4200000000000000000000000000000000000042'
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
op_buys AS (
    SELECT
        DISTINCT tx_hash AS op_tx
    FROM
        base
    WHERE
        function_type = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
),
currency_type AS (
    SELECT
        A.*,
        CASE
            WHEN op_tx IS NULL THEN 'ETH'
            ELSE 'OP'
        END AS currency_symbol
    FROM
        base A
        LEFT JOIN op_buys
        ON tx_hash = op_tx
    WHERE
        function_type = '0x70ba0d31158674eea8365d0f7b9ac70e552cc28b8bb848664e4feb939c6578f8'
),
hourly_prices AS (
    SELECT
        HOUR,
        CASE
            WHEN symbol = 'WETH' THEN 'ETH'
            ELSE symbol
        END AS symbol,
        token_address,
        price AS token_price
    FROM
        {{ ref('silver__prices') }}
    WHERE
        HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                currency_type
        )
        AND symbol IN (
            'OP',
            'WETH'
        )
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    seller_address,
    buyer_address,
    nft_address,
    tokenId,
    currency_symbol,
    CASE
        WHEN currency_symbol = 'ETH' THEN 'ETH'
        WHEN currency_symbol = 'OP' THEN '0x4200000000000000000000000000000000000042'
        ELSE token_address
    END AS currency_address,
    price,
    ROUND(
        token_price * price,
        2
    ) AS price_usd,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    _log_id,
    _inserted_timestamp
FROM
    currency_type
    LEFT JOIN hourly_prices
    ON DATE_TRUNC(
        'HOUR',
        block_timestamp
    ) = HOUR
    AND symbol = currency_symbol
WHERE
    nft_address <> '0xbe81eabdbd437cba43e4c1c330c63022772c2520' -- funky address throwing sales off with some weird events - ideally this is filtered to
    -- specific exchange addresses but i cant find common ones
    qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC) = 1)
