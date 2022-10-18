{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base_sales AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        CASE
            WHEN origin_from_address = COALESCE (
                event_inputs :_from :: STRING,
                event_inputs :from :: STRING
            ) THEN 'bid_won'
            ELSE 'sale'
        END AS event_type,
        origin_to_address AS platform_address,
        COALESCE (
            event_inputs :_from :: STRING,
            event_inputs :from :: STRING
        ) AS seller_address,
        COALESCE (
            event_inputs :_to :: STRING,
            event_inputs :to :: STRING
        ) AS buyer_address,
        contract_address AS nft_address,
        COALESCE (
            event_inputs :_id :: STRING,
            event_inputs :tokenId :: STRING
        ) AS tokenId,
        event_inputs :_value :: STRING AS erc1155_value,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        origin_function_signature IN (
            '0xb3a34c4c',
            '0xfb0f3ee1',
            '0x87201b41',
            '0x6e650cd4',
            '0x60f46aee',
            '0x12d0aa73',
            '0xbfc5e222',
            '0x2047d4d4',
            '0x55e4a3fa',
            '0xedf8301b',
            '0xad6c8c5f',
            '0xf8056016',
            '0x75eeb98a',
            '0x912d97fc'
        )
        AND origin_to_address IN (
            '0x998ef16ea4111094eb5ee72fc2c6f4e6e8647666',
            '0xe5c7b4865d7f2b08faadf3f6d392e6d6fa7b903c',
            '0x829b1c7b9d024a3915215b8abf5244a4bfc28db4',
            '0x20975da6eb930d592b9d78f451a9156db5e4c77b',
            '0x065e8a87b8f11aed6facf9447abe5e8c5d7502b6',
            '0x3f9da045b0f77d707ea4061110339c4ea8ecfa70'
        )
        AND block_timestamp >= '2021-01-01'
        AND event_name IN (
            'Transfer',
            'TransferSingle'
        )
        AND tokenId IS NOT NULL
        AND tx_status = 'SUCCESS'

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
optimism_campaign_tx_seller AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_to_address AS platform_address,
        COALESCE (
            event_inputs :_from :: STRING,
            event_inputs :from :: STRING
        ) AS seller_address,
        COALESCE (
            event_inputs :_id :: STRING,
            event_inputs :tokenId :: STRING
        ) AS tokenId,
        event_inputs :_value :: STRING AS erc1155_value,
        contract_address AS nft_address,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        _log_id,
        _inserted_timestamp,
        CASE
            WHEN origin_from_address = seller_address THEN 'bid_won'
            ELSE 'sale'
        END AS event_type
    FROM
        {{ ref('silver__logs') }}
    WHERE
        origin_function_signature IN (
            '0x6e650cd4'
        )
        AND origin_to_address IN ('0xc78a09d6a4badecc7614a339fd264b7290361ef1')
        AND block_timestamp >= '2021-01-01'
        AND event_name IN (
            'Transfer',
            'TransferSingle'
        )
        AND tokenId IS NOT NULL
        AND tx_status = 'SUCCESS'
        AND COALESCE (
            event_inputs :_to :: STRING,
            event_inputs :to :: STRING
        ) = '0xc78a09d6a4badecc7614a339fd264b7290361ef1'

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
optimism_campaign_tx_buyer AS (
    SELECT
        tx_hash,
        _inserted_timestamp,
        COALESCE (
            event_inputs :_to :: STRING,
            event_inputs :to :: STRING
        ) AS buyer_address,
        contract_address AS nft_address,
        COALESCE (
            event_inputs :_id :: STRING,
            event_inputs :tokenId :: STRING
        ) AS tokenId
    FROM
        {{ ref('silver__logs') }}
    WHERE
        origin_function_signature IN (
            '0x6e650cd4'
        )
        AND origin_to_address IN ('0xc78a09d6a4badecc7614a339fd264b7290361ef1')
        AND block_timestamp >= '2021-01-01'
        AND event_name IN (
            'Transfer',
            'TransferSingle'
        )
        AND tokenId IS NOT NULL
        AND tx_status = 'SUCCESS'
        AND COALESCE (
            event_inputs :_from :: STRING,
            event_inputs :from :: STRING
        ) = '0xc78a09d6a4badecc7614a339fd264b7290361ef1'

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
optimism_campaign_tx AS (
    SELECT
        block_number,
        block_timestamp,
        s.tx_hash,
        event_type,
        platform_address,
        seller_address,
        buyer_address,
        s.nft_address,
        s.tokenId,
        s.erc1155_value,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        _log_id,
        s._inserted_timestamp
    FROM
        optimism_campaign_tx_seller s
        INNER JOIN optimism_campaign_tx_buyer b
        ON s.tx_hash = b.tx_hash
        AND s.tokenId = b.tokenId
        AND s.nft_address = b.nft_address
),
agg_sales_tx AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_type,
        platform_address,
        seller_address,
        buyer_address,
        nft_address,
        tokenId,
        erc1155_value,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        _log_id
    FROM
        base_sales
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_type,
        platform_address,
        seller_address,
        buyer_address,
        nft_address,
        tokenId,
        erc1155_value,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        _log_id
    FROM
        optimism_campaign_tx
),
eth_sales_raw AS (
    SELECT
        f.block_number,
        t.block_timestamp,
        t.tx_hash,
        event_type,
        platform_address,
        seller_address,
        buyer_address,
        nft_address,
        tokenId,
        erc1155_value,
        'ETH' AS currency_address,
        COALESCE (
            CASE
                WHEN to_address = LOWER('0xeC1557A67d4980C948cD473075293204F4D280fd') THEN t.eth_value
            END,
            0
        ) AS platform_fee_raw,
        COALESCE (
            CASE
                WHEN to_address = seller_address THEN eth_value
            END,
            0
        ) AS sale_price_raw,
        COALESCE (
            CASE
                WHEN to_address != seller_address
                AND to_address != LOWER('0xeC1557A67d4980C948cD473075293204F4D280fd') THEN t.eth_value
            END,
            0
        ) AS creator_fee_raw,
        f.origin_from_address,
        f.origin_to_address,
        f.origin_function_signature,
        f._log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__traces') }}
        t
        INNER JOIN agg_sales_tx f
        ON t.tx_hash = f.tx_hash
    WHERE
        t.block_timestamp >= '2021-01-01'
        AND seller_address IS NOT NULL
        AND nft_address IS NOT NULL
        AND t.eth_value > 0
        AND identifier != 'CALL_ORIGIN'
        AND from_address != '0xc78a09d6a4badecc7614a339fd264b7290361ef1'

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
eth_sales AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_type,
        platform_address,
        seller_address,
        buyer_address,
        nft_address,
        tokenId,
        erc1155_value,
        currency_address,
        SUM(platform_fee_raw) AS platform_fee,
        SUM(sale_price_raw) AS sale_price,
        SUM(creator_fee_raw) AS creator_fee,
        platform_fee + sale_price + creator_fee AS price,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        _log_id,
        _inserted_timestamp
    FROM
        eth_sales_raw
    GROUP BY
        block_number,
        block_timestamp,
        tx_hash,
        event_type,
        platform_address,
        seller_address,
        buyer_address,
        nft_address,
        tokenId,
        erc1155_value,
        currency_address,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        _log_id,
        _inserted_timestamp
),
token_sales_raw AS (
    SELECT
        f.block_number,
        t.block_timestamp,
        t.tx_hash,
        event_type,
        platform_address,
        seller_address,
        buyer_address,
        nft_address,
        tokenId,
        erc1155_value,
        contract_address AS currency_address,
        COALESCE (
            CASE
                WHEN event_inputs :to = LOWER('0xeC1557A67d4980C948cD473075293204F4D280fd') THEN t.event_inputs :value / 1e18
            END,
            0
        ) AS platform_fee_raw,
        COALESCE (
            CASE
                WHEN event_inputs :to = seller_address THEN t.event_inputs :value / 1e18
            END,
            0
        ) AS sale_price_raw,
        COALESCE (
            CASE
                WHEN event_inputs :to != seller_address
                AND event_inputs :to != LOWER('0xeC1557A67d4980C948cD473075293204F4D280fd') THEN t.event_inputs :value / 1e18
            END,
            0
        ) AS creator_fee_raw,
        f.origin_from_address,
        f.origin_to_address,
        f.origin_function_signature,
        f._log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        t
        INNER JOIN agg_sales_tx f
        ON t.tx_hash = f.tx_hash
    WHERE
        t.block_timestamp >= '2021-01-01'
        AND t.tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                eth_sales
        )
        AND f.seller_address IS NOT NULL
        AND t.event_inputs :value IS NOT NULL
        AND nft_address IS NOT NULL
        AND event_name = 'Transfer'

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
token_sales AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_type,
        platform_address,
        seller_address,
        buyer_address,
        nft_address,
        tokenId,
        erc1155_value,
        currency_address,
        SUM(platform_fee_raw) AS platform_fee,
        SUM(sale_price_raw) AS sale_price,
        SUM(creator_fee_raw) AS creator_fee,
        platform_fee + sale_price + creator_fee AS price,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        _log_id,
        _inserted_timestamp
    FROM
        token_sales_raw
    GROUP BY
        block_number,
        block_timestamp,
        tx_hash,
        event_type,
        platform_address,
        seller_address,
        buyer_address,
        nft_address,
        tokenId,
        erc1155_value,
        currency_address,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        _log_id,
        _inserted_timestamp
),
agg_sales AS (
    SELECT
        *
    FROM
        eth_sales
    UNION ALL
    SELECT
        *
    FROM
        token_sales
),
hourly_prices AS (
    SELECT
        HOUR,
        symbol,
        CASE
            WHEN token_address IS NULL
            AND symbol = 'ETH' THEN 'ETH'
            ELSE token_address
        END AS currency_address,
        price AS token_price
    FROM
        {{ ref('silver__prices') }}
    WHERE
        HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                agg_sales_tx
        )
        AND currency_address IN (
            SELECT
                DISTINCT currency_address
            FROM
                agg_sales
        )
),
agg_sales_prices AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_type,
        platform_address,
        'quixotic' AS platform_name,
        'quixotic' AS platform_exchange_version,
        seller_address,
        buyer_address,
        nft_address,
        tokenId,
        erc1155_value,
        symbol AS currency_symbol,
        s.currency_address,
        price,
        ROUND(
            h.token_price * price,
            2
        ) AS price_usd,
        platform_fee,
        creator_fee,
        platform_fee + creator_fee AS total_fees,
        platform_fee * h.token_price AS platform_fee_usd,
        creator_fee * h.token_price AS creator_fee_usd,
        total_fees * h.token_price AS total_fees_usd,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        _log_id,
        _inserted_timestamp
    FROM
        agg_sales s
        LEFT JOIN hourly_prices h
        ON h.hour = DATE_TRUNC(
            'hour',
            s.block_timestamp
        )
        AND h.currency_address = s.currency_address
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    seller_address,
    buyer_address,
    nft_address,
    tokenId,
    erc1155_value,
    CASE
        WHEN currency_address = '0x4200000000000000000000000000000000000042' THEN 'OP'
        ELSE currency_symbol
    END AS currency_symbol,
    currency_address,
    price,
    price_usd,
    total_fees,
    platform_fee,
    creator_fee,
    total_fees_usd,
    platform_fee_usd,
    creator_fee_usd,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    _log_id,
    _inserted_timestamp
FROM
    agg_sales_prices qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC) = 1)
