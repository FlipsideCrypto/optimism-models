{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH nft_counts AS (

    SELECT
        tx_hash,
        CASE
            WHEN topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' 
                THEN TRY_TO_NUMBER(utils.udf_hex_to_int(topics [3] :: STRING))
        END AS transfer_token_id, 
        CASE
            WHEN topics [0] :: STRING = '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62' 
                THEN TRY_TO_NUMBER(utils.udf_hex_to_int(regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}')[0] :: STRING))
        END AS transfer_single_token_id
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_timestamp >= '2022-06-28'
        AND origin_to_address = LOWER('0x00000000006c3852cbEf3e08E8dF289169EdE581') -- seaport
        AND origin_function_signature NOT IN (
            '0xfd9f1e10',
            '0x5b34b966'
        )
        AND topics[0] :: STRING IN (
            '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
            '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
            ) -- Transfer, TransferSingle
        AND COALESCE (
            transfer_single_token_id,
            transfer_token_id
        ) IS NOT NULL
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
nft_count_transaction AS (
SELECT
    tx_hash,
    COUNT(
        DISTINCT COALESCE (
            transfer_single_token_id,
            transfer_token_id
        )
    ) AS nft_count
FROM nft_counts
GROUP BY
    tx_hash
),
base_sales AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address AS nft_address,
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
            transfer_single_to,
            transfer_to
        ) AS buyer_address,
        COALESCE (
            transfer_single_token_id,
            transfer_token_id
        ) AS tokenId,
        transfer_single_erc1155_value AS erc1155_value,
        CASE
            WHEN origin_from_address = seller_address THEN 'bid_won'
            WHEN origin_from_address = buyer_address THEN 'sale'
            ELSE 'sale'
        END AS event_type,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_timestamp >= '2022-06-28'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                nft_count_transaction
        )
        AND tx_status = 'SUCCESS'
        AND topics[0] :: STRING IN (
            '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
            '0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62'
            ) -- Transfer, TransferSingle
        AND origin_function_signature NOT IN (
            '0xfd9f1e10',
            '0x5b34b966'
        )
        AND (
            transfer_single_token_id IS NOT NULL
            OR transfer_token_id IS NOT NULL
        )
        AND seller_address != '0x0000000000000000000000000000000000000000'

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
base_sales_without_inserted_timestamp AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        nft_address,
        seller_address,
        buyer_address,
        tokenId,
        erc1155_value,
        event_type,
        _log_id
    FROM
        base_sales
),
eth_sales_raw AS (
    SELECT
        b.block_number,
        b.block_timestamp,
        b._log_id,
        _inserted_timestamp,
        t.tx_hash,
        b.origin_function_signature,
        b.origin_from_address,
        b.origin_to_address,
        b.nft_address,
        seller_address,
        buyer_address,
        tokenId,
        erc1155_value,
        event_type,
        COALESCE (
            CASE
                WHEN to_address = LOWER('0x0000a26b00c1F0DF003000390027140000fAa719') THEN eth_value
            END,
            0
        ) AS platform_fee_raw,
        COALESCE (
            CASE
                WHEN to_address = seller_address THEN eth_value
            END,
            0
        ) AS price_raw,
        COALESCE (
            CASE
                WHEN to_address != seller_address
                AND to_address != LOWER('0x0000a26b00c1F0DF003000390027140000fAa719') THEN eth_value
            END,
            0
        ) AS creator_fee_raw
    FROM
        {{ ref('silver__traces') }}
        t
        INNER JOIN base_sales_without_inserted_timestamp b
        ON t.tx_hash = b.tx_hash
    WHERE
        t.block_timestamp >= '2022-06-28'
        AND t.eth_value > 0
        AND identifier != 'CALL_ORIGIN'

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
        _log_id,
        _inserted_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        nft_address,
        seller_address,
        buyer_address,
        tokenId,
        erc1155_value,
        event_type,
        'ETH' AS currency_address,
        SUM(price_raw) AS price,
        SUM(platform_fee_raw) AS platform_fee,
        SUM(creator_fee_raw) AS creator_fee,
        platform_fee + creator_fee AS total_fees
    FROM
        eth_sales_raw
    GROUP BY
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        nft_address,
        seller_address,
        buyer_address,
        tokenId,
        erc1155_value,
        event_type,
        currency_address,
        block_number,
        block_timestamp,
        _log_id,
        _inserted_timestamp
),
token_sales_raw AS (
    SELECT
        b.block_number,
        b._log_id,
        _inserted_timestamp,
        b.block_timestamp,
        t.tx_hash,
        b.origin_function_signature,
        b.origin_from_address,
        b.origin_to_address,
        b.nft_address,
        seller_address,
        buyer_address,
        tokenId,
        erc1155_value,
        event_type,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS transfer_to,
        TRY_TO_NUMBER(utils.udf_hex_to_int(regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}')[0] :: STRING)) AS transfer_value,
        LOWER(
            t.contract_address
        ) AS currency_address,
        COALESCE (
            CASE
                WHEN transfer_to = LOWER('0x0000a26b00c1F0DF003000390027140000fAa719') THEN transfer_value
            END / 1e18,
            0
        ) AS platform_fee_raw,
        COALESCE (
            CASE
                WHEN transfer_to = seller_address THEN transfer_value
            END / 1e18,
            0
        ) AS price_raw,
        COALESCE (
            CASE
                WHEN transfer_to != seller_address
                AND transfer_to != LOWER('0x0000a26b00c1F0DF003000390027140000fAa719') THEN transfer_value
            END / 1e18,
            0
        ) AS creator_fee_raw
    FROM
        {{ ref('silver__logs') }}
        t
        INNER JOIN base_sales_without_inserted_timestamp b
        ON t.tx_hash = b.tx_hash
    WHERE
        t.block_timestamp >= '2022-06-28'
        AND transfer_value IS NOT NULL
        AND topics[0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' -- Transfer

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
        _log_id,
        _inserted_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        nft_address,
        seller_address,
        buyer_address,
        tokenId,
        erc1155_value,
        event_type,
        currency_address,
        SUM(price_raw) AS price,
        SUM(platform_fee_raw) AS platform_fee,
        SUM(creator_fee_raw) AS creator_fee,
        platform_fee + creator_fee AS total_fees
    FROM
        token_sales_raw
    GROUP BY
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        nft_address,
        seller_address,
        buyer_address,
        tokenId,
        erc1155_value,
        event_type,
        currency_address,
        block_number,
        block_timestamp,
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
        decimals,
        symbol,
        token_address AS currency_address,
        price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        (
            currency_address IN (
                SELECT
                    DISTINCT currency_address
                FROM
                    agg_sales
            )
        )
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                base_sales
        )
),
eth_price AS (
    SELECT
        HOUR,
        decimals,
        'ETH' AS symbol,
        'ETH' AS currency_address,
        price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        token_address = '0x4200000000000000000000000000000000000006'
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                base_sales
        )
),
all_prices AS (
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
agg_sales_prices AS (
    SELECT
        s.block_number,
        s.block_timestamp,
        s._log_id,
        s._inserted_timestamp,
        s.tx_hash,
        s.event_type,
        s.origin_to_address AS platform_address,
        'opensea' AS platform_name,
        'seaport_1_1' AS platform_exchange_version,
        seller_address,
        buyer_address,
        s.nft_address,
        s.tokenId,
        s.erc1155_value,
        p.symbol AS currency_symbol,
        s.currency_address,
        CASE
            WHEN s.currency_address = 'ETH'
            OR s.currency_address = '0x4200000000000000000000000000000000000006' THEN s.price / n.nft_count
            WHEN s.currency_address != 'ETH'
            AND p.currency_address IS NOT NULL THEN (s.price / pow(10, decimals)) / n.nft_count
            WHEN p.currency_address IS NULL THEN s.price / n.nft_count
        END AS prices,
        ROUND (
            prices * p.price,
            2
        ) AS price_usd,
        CASE
            WHEN s.currency_address = 'ETH'
            OR s.currency_address = '0x4200000000000000000000000000000000000006' THEN total_fees / n.nft_count
            WHEN s.currency_address != 'ETH'
            AND p.currency_address IS NOT NULL THEN (total_fees / pow(10, decimals)) / n.nft_count
            WHEN p.currency_address IS NULL THEN total_fees / n.nft_count
        END AS total_fees_adj,
        CASE
            WHEN s.currency_address = 'ETH'
            OR s.currency_address = '0x4200000000000000000000000000000000000006' THEN platform_fee / n.nft_count
            WHEN s.currency_address != 'ETH'
            AND p.currency_address IS NOT NULL THEN (platform_fee / pow(10, decimals)) / n.nft_count
            WHEN p.currency_address IS NULL THEN platform_fee / n.nft_count
        END AS platform_fee_adj,
        CASE
            WHEN s.currency_address = 'ETH'
            OR s.currency_address = '0x4200000000000000000000000000000000000006' THEN creator_fee / n.nft_count
            WHEN s.currency_address != 'ETH'
            AND p.currency_address IS NOT NULL THEN (creator_fee / pow(10, decimals)) / n.nft_count
            WHEN p.currency_address IS NULL THEN creator_fee / n.nft_count
        END AS creator_fee_adj,
        total_fees_adj * p.price AS total_fees_usd,
        platform_fee_adj * p.price AS platform_fee_usd,
        creator_fee_adj * p.price AS creator_fee_usd,
        prices + total_fees_adj AS total_transaction_price,
        price_usd + total_fees_usd AS total_transaction_price_usd,
        s.origin_from_address,
        s.origin_to_address,
        s.origin_function_signature
    FROM
        agg_sales s
        INNER JOIN nft_count_transaction n
        ON n.tx_hash = s.tx_hash
        LEFT JOIN all_prices p
        ON DATE_TRUNC(
            'hour',
            s.block_timestamp
        ) = p.hour
        AND s.currency_address = p.currency_address
    WHERE
        s.block_number IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY _log_id
    ORDER BY
        _inserted_timestamp DESC)) = 1
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
        WHEN currency_address = '0x4200000000000000000000000000000000000006' THEN 'WETH'
        WHEN currency_address = 'ETH' THEN 'ETH'
        ELSE currency_symbol
    END AS currency_symbol,
    currency_address,
    total_transaction_price AS price,
    total_transaction_price_usd AS price_usd,
    total_fees_adj AS total_fees,
    platform_fee_adj AS platform_fee,
    creator_fee_adj AS creator_fee,
    total_fees_usd,
    platform_fee_usd,
    creator_fee_usd,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    _log_id,
    _inserted_timestamp
FROM
    agg_sales_prices
