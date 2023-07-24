{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH seaport_tx AS (

    SELECT
        tx_hash
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_timestamp >= '2021-01-01'
        AND topics [0] :: STRING = '0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31'
        AND origin_to_address != LOWER('0x00000000006c3852cbEf3e08E8dF289169EdE581')

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
quixotic_tx AS (
    SELECT
        tx_hash
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_timestamp >= '2021-01-01'
        AND origin_function_signature IN (
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
            '0xe5c7b4865d7f2b08faadf3f6d392e6d6fa7b903c',
            '0x829b1c7b9d024a3915215b8abf5244a4bfc28db4',
            '0x20975da6eb930d592b9d78f451a9156db5e4c77b',
            '0x065e8a87b8f11aed6facf9447abe5e8c5d7502b6',
            '0x3f9da045b0f77d707ea4061110339c4ea8ecfa70'
        )

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
seaport_indirect_contract_tx AS (
    SELECT
        tx_hash
    FROM
        seaport_tx
    UNION
    SELECT
        tx_hash
    FROM
        quixotic_tx
),
base_sales AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        CASE
            WHEN topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' 
                THEN CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) 
        END AS transfer_from,
        CASE
            WHEN topics [0] :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62' 
                THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))
        END AS transfer_single_from,
        CASE
            WHEN topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' 
                THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) 
        END AS transfer_to,
        CASE
            WHEN topics [0] :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62' 
                THEN CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40))
        END AS transfer_single_to,
        CASE
            WHEN topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' 
                THEN TRY_TO_NUMBER(utils.udf_hex_to_int(topics [3] :: STRING))
        END AS transfer_token_id, 
        CASE
            WHEN topics [0] :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62' 
                THEN TRY_TO_NUMBER(utils.udf_hex_to_int(regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}')[0] :: STRING))
        END AS transfer_single_token_id,
        CASE
            WHEN topics [0] :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62' 
                THEN TRY_TO_NUMBER(utils.udf_hex_to_int(regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}')[1] :: STRING)) 
        END AS transfer_single_erc1155_value,
        CASE
            WHEN origin_from_address = COALESCE (
                transfer_single_from,
                transfer_from
            ) THEN 'bid_won'
            ELSE 'sale'
        END AS event_type,
        origin_to_address AS platform_address,
        COALESCE (
            transfer_single_from,
            transfer_from
        ) AS seller_address,
        COALESCE (
            transfer_single_to,
            transfer_to
        ) AS buyer_address,
        contract_address AS nft_address,
        COALESCE (
            transfer_single_token_id,
            transfer_token_id
        ) AS tokenId,
        transfer_single_erc1155_value AS erc1155_value,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_timestamp >= '2021-01-01'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                seaport_indirect_contract_tx
        )
        AND topics[0] :: STRING IN (
            '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
            '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
            ) -- Transfer, TransferSingle
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
        CASE
            WHEN topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' 
                THEN CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) 
        END AS transfer_from,
        CASE
            WHEN topics [0] :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62' 
                THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))
        END AS transfer_single_from,
        CASE
            WHEN topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' 
                THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) 
        END AS transfer_to,
        CASE
            WHEN topics [0] :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62' 
                THEN CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40))
        END AS transfer_single_to,
        CASE
            WHEN topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' 
                THEN TRY_TO_NUMBER(utils.udf_hex_to_int(topics [3] :: STRING))
        END AS transfer_token_id, 
        CASE
            WHEN topics [0] :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62' 
                THEN TRY_TO_NUMBER(utils.udf_hex_to_int(regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}')[0] :: STRING))
        END AS transfer_single_token_id,
        CASE
            WHEN topics [0] :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62' 
                THEN TRY_TO_NUMBER(utils.udf_hex_to_int(regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}')[1] :: STRING)) 
        END AS transfer_single_erc1155_value,
        COALESCE (
            transfer_single_from,
            transfer_from
        ) AS seller_address,
        COALESCE (
            transfer_single_token_id,
            transfer_token_id
        ) AS tokenId,
        transfer_single_erc1155_value AS erc1155_value,
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
            '0x6e650cd4',
            '0xac6e389e'
        )
        AND origin_to_address IN ('0xc78a09d6a4badecc7614a339fd264b7290361ef1')
        AND block_timestamp >= '2021-01-01'
        AND topics[0] :: STRING IN (
            '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
            '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
            ) 
        AND tokenId IS NOT NULL
        AND tx_status = 'SUCCESS'
        AND COALESCE (
            transfer_single_to,
            transfer_to
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
        CASE
            WHEN topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' 
                THEN CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) 
        END AS transfer_from,
        CASE
            WHEN topics [0] :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62' 
                THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))
        END AS transfer_single_from,
        CASE
            WHEN topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' 
                THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) 
        END AS transfer_to,
        CASE
            WHEN topics [0] :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62' 
                THEN CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40))
        END AS transfer_single_to,
        CASE
            WHEN topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' 
                THEN TRY_TO_NUMBER(utils.udf_hex_to_int(topics [3] :: STRING))
        END AS transfer_token_id, 
        CASE
            WHEN topics [0] :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62' 
                THEN TRY_TO_NUMBER(utils.udf_hex_to_int(regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}')[0] :: STRING))
        END AS transfer_single_token_id,
        COALESCE (
            transfer_single_to,
            transfer_to
        ) AS buyer_address,
        contract_address AS nft_address,
        COALESCE (
            transfer_single_token_id,
            transfer_token_id
        ) AS tokenId
    FROM
        {{ ref('silver__logs') }}
    WHERE
        origin_function_signature IN (
            '0x6e650cd4',
            '0xac6e389e'
        )
        AND origin_to_address IN ('0xc78a09d6a4badecc7614a339fd264b7290361ef1')
        AND block_timestamp >= '2021-01-01'
        AND topics[0] :: STRING IN (
            '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
            '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
            ) 
        AND tokenId IS NOT NULL
        AND tx_status = 'SUCCESS'
        AND COALESCE (
            transfer_single_from,
            transfer_from
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
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS transfer_to,
        TRY_TO_NUMBER(utils.udf_hex_to_int(regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}')[0] :: STRING)) AS transfer_value,
        COALESCE (
            CASE
                WHEN transfer_to = LOWER('0xeC1557A67d4980C948cD473075293204F4D280fd') THEN transfer_value / 1e18
            END,
            0
        ) AS platform_fee_raw,
        COALESCE (
            CASE
                WHEN transfer_to = seller_address THEN transfer_value / 1e18
            END,
            0
        ) AS sale_price_raw,
        COALESCE (
            CASE
                WHEN transfer_to != seller_address
                AND transfer_to != LOWER('0xeC1557A67d4980C948cD473075293204F4D280fd') THEN transfer_value / 1e18
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
        AND transfer_value IS NOT NULL
        AND nft_address IS NOT NULL
        AND topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

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
token_prices AS (
    SELECT
        HOUR,
        symbol,
        token_address AS currency_address,
        price AS token_price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                agg_sales_tx
        )
        AND (
            token_address IN (
                SELECT
                    DISTINCT currency_address
                FROM
                    agg_sales
            )
            OR token_address = '0x4200000000000000000000000000000000000006'
        )
),
eth_price AS (
    SELECT
        HOUR,
        'ETH' AS symbol,
        'ETH' AS currency_address,
        price AS token_price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        token_address = '0x4200000000000000000000000000000000000006'
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                agg_sales_tx
        )
),
hourly_prices AS (
    SELECT
        *
    FROM
        token_prices
    UNION ALL
    SELECT
        *
    FROM
        eth_price
),
nfts_per AS (
    SELECT
        tx_hash,
        COUNT(*) AS nfts_per_tx
    FROM
        agg_sales
    GROUP BY
        tx_hash
),
agg_sales_prices AS (
    SELECT
        block_number,
        block_timestamp,
        s.tx_hash,
        event_type,
        platform_address,
        'quixotic' AS platform_name,
        seller_address,
        buyer_address,
        nft_address,
        tokenId,
        erc1155_value,
        symbol AS currency_symbol,
        s.currency_address,
        s.price / nfts_per_tx AS price,
        ROUND(
            h.token_price * (
                s.price / nfts_per_tx
            ),
            2
        ) AS price_usd,
        platform_fee / nfts_per_tx AS platform_fee,
        creator_fee / nfts_per_tx AS creator_fee,
        (
            platform_fee / nfts_per_tx
        ) + (
            creator_fee / nfts_per_tx
        ) AS total_fees,
        (
            platform_fee / nfts_per_tx
        ) * h.token_price AS platform_fee_usd,
        (
            creator_fee / nfts_per_tx
        ) * h.token_price AS creator_fee_usd,
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
        AND h.currency_address = (
            CASE
                WHEN s.currency_address = 'ETH' THEN '0x4200000000000000000000000000000000000006'
                ELSE s.currency_address
            END
        )
        LEFT JOIN nfts_per
        ON nfts_per.tx_hash = s.tx_hash
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    CASE
        WHEN tx_hash IN (
            SELECT
                tx_hash
            FROM
                seaport_tx
        ) THEN 'seaport_1_1'
        ELSE 'quixotic'
    END AS platform_exchange_version,
    seller_address,
    buyer_address,
    nft_address,
    tokenId,
    erc1155_value,
    CASE
        WHEN currency_address = '0x4200000000000000000000000000000000000042' THEN 'OP'
        WHEN currency_address = '0x4200000000000000000000000000000000000006' THEN 'WETH'
        WHEN currency_address = 'ETH' THEN 'ETH'
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
