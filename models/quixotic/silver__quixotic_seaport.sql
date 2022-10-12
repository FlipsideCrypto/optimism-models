{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}


with nft_count_transaction as (
select 
    tx_hash, 
    count(distinct coalesce (event_inputs:_id ::string , event_inputs:tokenId ::string)) as nft_count 
from {{ ref('silver__logs') }}
where block_timestamp >= '2022-06-28'
and origin_to_address = lower('0x00000000006c3852cbEf3e08E8dF289169EdE581') -- seaport 
and origin_function_signature not in (
    '0xfd9f1e10'
    ,'0x5b34b966'
    )

and event_name in ('Transfer', 'TransferSingle')
and coalesce (event_inputs:_id ::string , event_inputs:tokenId ::string) is not null 
and tx_status = 'SUCCESS'
    group by tx_hash 
    ), 
    
base_sales as (            
select 
    block_number,
    block_timestamp,
    tx_hash, 
    origin_function_signature, 
    origin_from_address, 
    origin_to_address,
    contract_address as nft_address,
    coalesce (event_inputs:_from ::string , event_inputs:from ::string) as seller_address,
    coalesce (event_inputs:_to ::string , event_inputs:to :: string) as buyer_address,
    coalesce (event_inputs:_id ::string , event_inputs:tokenId ::string) as tokenId,
    event_inputs:_value::string as erc1155_value, 
    case when origin_from_address = seller_address then 'bid_won'
        when origin_from_address = buyer_address then 'sale'
        else 'sale' end as event_type,
    _log_id,
    _inserted_timestamp
    
from {{ ref('silver__logs') }}
    
    WHERE block_timestamp >= '2022-06-28'
    and tx_hash in (select tx_hash from nft_count_transaction)
   
    AND tx_status = 'SUCCESS'
    and event_name in ('Transfer', 'TransferSingle')
    and origin_function_signature not in (
    '0xfd9f1e10'
    ,'0x5b34b966'
    )
    and (event_inputs:_id is not null or event_inputs:tokenId is not null)
    and seller_address != '0x0000000000000000000000000000000000000000'
    
    ),
    
eth_sales as (
    
    select 
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
    'ETH' as currency_address,
    sum(price_raw) as price,
    sum(platform_fee_raw) as platform_fee,
    sum(creator_fee_raw) as creator_fee,
    platform_fee + creator_fee as total_fees
    
    from ( 
    select 
    b.block_number,
    b.block_timestamp,
    b._log_id,
    b._inserted_timestamp,
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
        
    coalesce (case when to_address = lower('0x0000a26b00c1F0DF003000390027140000fAa719')
              then eth_value end , 0) as platform_fee_raw,
    coalesce (case when to_address = seller_address then eth_value end , 0) as price_raw,
    coalesce (case when to_address != seller_address 
            and to_address != lower('0x0000a26b00c1F0DF003000390027140000fAa719') 
            then eth_value end , 0) as creator_fee_raw 
    
    from {{ ref('silver__traces') }} t 
    inner join base_sales b on t.tx_hash = b.tx_hash 
    
    where t.block_timestamp >= '2022-06-28'
    and t.eth_value > 0
    and identifier != 'CALL_ORIGIN'

        )
    
    group by tx_hash, 
    origin_function_signature, 
    origin_from_address, 
    origin_to_address,
    nft_address, 
    seller_address,
    buyer_address,
    tokenId,
    erc1155_value, event_type, currency_address, block_number, 
    block_timestamp, 
    _log_id, 
    _inserted_timestamp
    
    ),
    
token_sales as (
    
    select 
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
    sum(price_raw) as price,
    sum(platform_fee_raw) as platform_fee,
    sum(creator_fee_raw) as creator_fee,
    platform_fee + creator_fee as total_fees
    
    from ( 
    select 
    b.block_number, 
    b._log_id, 
    b._inserted_timestamp,
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
    lower(t.contract_address) as currency_address,
    coalesce (case when event_inputs:to = lower('0x0000a26b00c1F0DF003000390027140000fAa719') 
              then event_inputs:value end /1e18, 0) as platform_fee_raw,
    coalesce (case when event_inputs:to = seller_address then event_inputs:value end /1e18 , 0) as price_raw,
    coalesce (case when event_inputs:to != seller_address 
            and event_inputs:to != lower('0x0000a26b00c1F0DF003000390027140000fAa719') 
            then event_inputs:value end /1e18 , 0) as creator_fee_raw
    from {{ ref('silver__logs') }} t 
    inner join base_sales b on t.tx_hash = b.tx_hash 
    
    where t.block_timestamp >= '2022-06-28'
    and event_inputs:value is not null 
    and event_name = 'Transfer'

        )
    
    group by tx_hash, 
    origin_function_signature, 
    origin_from_address, 
    origin_to_address,
    nft_address, 
    seller_address,
    buyer_address,
    tokenId,
    erc1155_value, event_type, currency_address, block_number, 
    block_timestamp, 
    _log_id, 
    _inserted_timestamp
    
    ),
    
    agg_sales as (
    select * from eth_sales 
    union all 
    select* from token_sales 
    ),

    all_prices AS (
    SELECT
        HOUR,
        decimals,
        symbol,
        CASE
            WHEN LOWER(token_address) IS NULL and symbol = 'ETH' THEN 'ETH'
            ELSE LOWER(token_address)
        END AS currency_address,
        AVG(price) AS price
    FROM
        {{ ref('silver__prices') }}
    WHERE
        (
            currency_address IN (
                SELECT 
                DISTINCT currency_address 
                FROM agg_sales 
        
            )
            
        )
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                base_sales
        )
    GROUP BY
        HOUR, decimals, 
        symbol,
        token_address
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
    
    case when s.currency_address = 'ETH' then s.price/ n.nft_count
        when s.currency_address != 'ETH' and p.currency_address is not null 
                then (s.price / pow(10, decimals)) / n.nft_count
        when p.currency_address is null then s.price / n.nft_count
            end as prices,
    
    round (prices * p.price , 2) as price_usd,
    
    case when s.currency_address = 'ETH' then total_fees / n.nft_count
        when s.currency_address != 'ETH' and p.currency_address is not null
            then (total_fees / pow(10, decimals)) / n.nft_count
        when p.currency_address is null then total_fees / n.nft_count
            end as total_fees_adj,
    
    case when s.currency_address = 'ETH' then platform_fee / n.nft_count
        when s.currency_address != 'ETH' and p.currency_address is not null
            then (platform_fee / pow(10, decimals)) / n.nft_count
        when p.currency_address is null then platform_fee / n.nft_count
            end as platform_fee_adj,
    
    case when s.currency_address = 'ETH' then creator_fee / n.nft_count
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

select
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
    currency_symbol,
    currency_address, 
    total_transaction_price as price,
    total_transaction_price_usd as price_usd,

    total_fees_adj as total_fees,
    platform_fee_adj as platform_fee,
    creator_fee_adj as creator_fee,
    total_fees_usd,
    platform_fee_usd,
    creator_fee_usd,

    origin_from_address, 
    origin_to_address,
    origin_function_signature, 
    _log_id,
    _inserted_timestamp

from agg_sales_prices