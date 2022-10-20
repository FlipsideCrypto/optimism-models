{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH nft_count_transaction AS (

    SELECT
        tx_hash,
        COUNT(
            DISTINCT COALESCE (
                event_inputs :_id :: STRING,
                event_inputs :tokenId :: STRING
            )
        ) AS nft_count
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_timestamp >= '2022-06-28'
        AND origin_to_address = LOWER('0x00000000006c3852cbEf3e08E8dF289169EdE581') -- seaport
        AND origin_function_signature NOT IN (
            '0xfd9f1e10',
            '0x5b34b966'
        )
        AND event_name IN (
            'Transfer',
            'TransferSingle'
        )
        AND COALESCE (
            event_inputs :_id :: STRING,
            event_inputs :tokenId :: STRING
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
        COALESCE (
            event_inputs :_from :: STRING,
            event_inputs :from :: STRING
        ) AS seller_address,
        COALESCE (
            event_inputs :_to :: STRING,
            event_inputs :to :: STRING
        ) AS buyer_address,
        COALESCE (
            event_inputs :_id :: STRING,
            event_inputs :tokenId :: STRING
        ) AS tokenId,
        event_inputs :_value :: STRING AS erc1155_value,
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
        AND event_name IN (
            'Transfer',
            'TransferSingle'
        )
        AND origin_function_signature NOT IN (
            '0xfd9f1e10',
            '0x5b34b966'
        )
        AND (
            event_inputs :_id IS NOT NULL
            OR event_inputs :tokenId IS NOT NULL
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
        LOWER(
            t.contract_address
        ) AS currency_address,
        COALESCE (
            CASE
                WHEN event_inputs :to = LOWER('0x0000a26b00c1F0DF003000390027140000fAa719') THEN event_inputs :value
            END / 1e18,
            0
        ) AS platform_fee_raw,
        COALESCE (
            CASE
                WHEN event_inputs :to = seller_address THEN event_inputs :value
            END / 1e18,
            0
        ) AS price_raw,
        COALESCE (
            CASE
                WHEN event_inputs :to != seller_address
                AND event_inputs :to != LOWER('0x0000a26b00c1F0DF003000390027140000fAa719') THEN event_inputs :value
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
        AND event_inputs :value IS NOT NULL
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
all_prices AS (
    SELECT
        HOUR,
        decimals,
        symbol,
        CASE
            WHEN LOWER(token_address) IS NULL
            AND symbol = 'ETH' THEN 'ETH'
            ELSE LOWER(token_address)
        END AS currency_address,
        price
    FROM
        {{ ref('silver__prices') }}
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


agg_sales_prices as (
select 
    s.block_number, 
    s.block_timestamp, 
    s._log_id, 
    s._inserted_timestamp,
    s.tx_hash, 
    s.event_type,
    s.origin_to_address as platform_address,
    'quixotic' as platform_name,
    'seaport_1_1' AS platform_exchange_version,
    
    seller_address,
    buyer_address,
    s.nft_address, 
    s.tokenId,
    s.erc1155_value,
    
    
    p.symbol as currency_symbol,
    s.currency_address, 
    
    case when s.currency_address = 'ETH' or s.currency_address = '0x4200000000000000000000000000000000000006'
        then s.price/ n.nft_count
        when s.currency_address != 'ETH' and p.currency_address is not null 
                then (s.price / pow(10, decimals)) / n.nft_count
        when p.currency_address is null then s.price / n.nft_count
            end as prices,
    
    round (prices * p.price , 2) as price_usd,
    
    case when s.currency_address = 'ETH' or s.currency_address = '0x4200000000000000000000000000000000000006'
        then total_fees / n.nft_count
        when s.currency_address != 'ETH' and p.currency_address is not null
            then (total_fees / pow(10, decimals)) / n.nft_count
        when p.currency_address is null then total_fees / n.nft_count
            end as total_fees_adj,
    
    case when s.currency_address = 'ETH' or s.currency_address = '0x4200000000000000000000000000000000000006'
        then platform_fee / n.nft_count
        when s.currency_address != 'ETH' and p.currency_address is not null
            then (platform_fee / pow(10, decimals)) / n.nft_count
        when p.currency_address is null then platform_fee / n.nft_count
            end as platform_fee_adj,
    
    case when s.currency_address = 'ETH' or s.currency_address = '0x4200000000000000000000000000000000000006'
        then creator_fee / n.nft_count
        when s.currency_address != 'ETH' and p.currency_address is not null
            then (creator_fee / pow(10, decimals))/ n.nft_count
        when p.currency_address is null then creator_fee / n.nft_count
            end as creator_fee_adj,
    
    total_fees_adj * p.price as total_fees_usd,
    platform_fee_adj * p.price as platform_fee_usd,
    creator_fee_adj * p.price as creator_fee_usd,
    prices + total_fees_adj as total_transaction_price,
    price_usd + total_fees_usd as total_transaction_price_usd,
    
    
    s.origin_from_address, 
    s.origin_to_address,
    s.origin_function_signature
    
    from agg_sales s 
        inner join nft_count_transaction n on n.tx_hash = s.tx_hash 
        left join all_prices p on date_trunc('hour', s.block_timestamp) = p.hour 
                and s.currency_address = p.currency_address
    
    where s.block_number is not null 
    
    qualify(ROW_NUMBER() over(PARTITION BY _log_id
                                    ORDER BY _inserted_timestamp DESC)
           ) = 1
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
