{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}


with base_sales as (
select 
    block_number,
    block_timestamp,
    tx_hash,
    case when origin_from_address = coalesce (event_inputs:_from ::string , event_inputs:from ::string) then 'bid_won'
        else 'sale' end as event_type,
    origin_to_address as platform_address,  
    
    coalesce (event_inputs:_from ::string , event_inputs:from ::string) as seller_address,
    coalesce (event_inputs:_to ::string , event_inputs:to :: string) as buyer_address,
    contract_address as nft_address,
    coalesce (event_inputs:_id ::string , event_inputs:tokenId ::string) as tokenId,
    event_inputs:_value::string as erc1155_value,
     
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    _log_id,
    _inserted_timestamp
    
    from {{ ref('silver__logs') }}
    where origin_function_signature in (
     '0xb3a34c4c'
,'0xfb0f3ee1'
,'0x87201b41'
,'0x6e650cd4' 
,'0x60f46aee'
,'0x12d0aa73'
,'0xbfc5e222'
,'0x2047d4d4'
,'0x55e4a3fa'
,'0xedf8301b'
,'0xad6c8c5f'
,'0xf8056016'
,'0x75eeb98a'
,'0x912d97fc' 
     ) 
    and origin_to_address in 
    (
      '0x998ef16ea4111094eb5ee72fc2c6f4e6e8647666' 
,'0xe5c7b4865d7f2b08faadf3f6d392e6d6fa7b903c' 
,'0x829b1c7b9d024a3915215b8abf5244a4bfc28db4'
,'0x20975da6eb930d592b9d78f451a9156db5e4c77b'
,'0x065e8a87b8f11aed6facf9447abe5e8c5d7502b6'
,'0x3f9da045b0f77d707ea4061110339c4ea8ecfa70'
    )
    
    and block_timestamp >= '2021-01-01' 
    and event_name in ('Transfer', 'TransferSingle')
    and tokenId is not null 
    and tx_status = 'SUCCESS'
    

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

    optimism_campaign_tx_seller as (
    select 
    block_number,
    block_timestamp,
    tx_hash,
        
    origin_to_address as platform_address, 
    
    coalesce (event_inputs:_from ::string , event_inputs:from ::string) as seller_address,
    coalesce (event_inputs:_id ::string , event_inputs:tokenId ::string) as tokenId,
    event_inputs:_value::string as erc1155_value,
    
    contract_address as nft_address, 
     
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    _log_id,
    _inserted_timestamp,
    
    case when origin_from_address = seller_address then 'bid_won'
        else 'sale' end as event_type 
    
    from {{ ref('silver__logs') }}
    where origin_function_signature in (
        '0x6e650cd4'
     ) 
    and origin_to_address in ('0xc78a09d6a4badecc7614a339fd264b7290361ef1')
    and block_timestamp >= '2021-01-01' 
    and event_name in ('Transfer', 'TransferSingle')
    and tokenId is not null 
    and tx_status = 'SUCCESS'
    and coalesce (event_inputs:_to ::string , event_inputs:to :: string) = '0xc78a09d6a4badecc7614a339fd264b7290361ef1'

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
    
    optimism_campaign_tx_buyer as (
    select 
    tx_hash,
    coalesce (event_inputs:_to ::string , event_inputs:to :: string) as buyer_address,    
    
    contract_address as nft_address, 
    coalesce (event_inputs:_id ::string , event_inputs:tokenId ::string) as tokenId
     
    from {{ ref('silver__logs') }}
    where origin_function_signature in (
        '0x6e650cd4'
     ) 
    and origin_to_address in ('0xc78a09d6a4badecc7614a339fd264b7290361ef1')
    and block_timestamp >= '2021-01-01' 
    and event_name in ('Transfer', 'TransferSingle')
    and tokenId is not null 
    and tx_status = 'SUCCESS'
    and coalesce (event_inputs:_from ::string , event_inputs:from ::string) = '0xc78a09d6a4badecc7614a339fd264b7290361ef1'
    ),
    
    optimism_campaign_tx as (
    select 
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
    _inserted_timestamp
        from optimism_campaign_tx_seller s 
        inner join optimism_campaign_tx_buyer b on s.tx_hash = b.tx_hash 
                                                and s.tokenId = b.tokenId 
                                                and s.nft_address = b.nft_address
    ),
    
    agg_sales_tx as (
    select * from base_sales 
    union all 
    select * from optimism_campaign_tx
    ),


eth_sales_raw as (  
    select 
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
    'ETH' as currency_address, 
    coalesce (case when to_address = lower('0xeC1557A67d4980C948cD473075293204F4D280fd')
              then t.eth_value end, 0) as platform_fee_raw,
    coalesce (case when to_address = seller_address then eth_value end , 0) as sale_price_raw,
    coalesce (case when to_address != seller_address and to_address != lower('0xeC1557A67d4980C948cD473075293204F4D280fd')
             then t.eth_value end, 0 ) as creator_fee_raw,
    
    f.origin_from_address,
    f.origin_to_address,
    f.origin_function_signature,
    f._log_id,
    f._inserted_timestamp
        
    from {{ ref('silver__traces') }} t 
    inner join agg_sales_tx f on t.tx_hash = f.tx_hash 
    where t.block_timestamp >= '2021-01-01'
    and seller_address is not null 
    and nft_address is not null 
    and t.eth_value > 0 
    and identifier != 'CALL_ORIGIN'
    and from_address != '0xc78a09d6a4badecc7614a339fd264b7290361ef1'


    ),
    
    eth_sales as (
    select 
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
    sum(platform_fee_raw) as platform_fee,
    sum(sale_price_raw) as sale_price,
    sum(creator_fee_raw) as creator_fee,
    platform_fee + sale_price + creator_fee as price,
    
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    _log_id,
    _inserted_timestamp
    from eth_sales_raw 
        
    group by block_number, 
    block_timestamp, 
    tx_hash,
    event_type, 
    platform_address, 
    seller_address,
    buyer_address,
    nft_address, 
    tokenId, erc1155_value,
     currency_address, origin_from_address,
    origin_to_address,
    origin_function_signature,
    _log_id,
    _inserted_timestamp
    ),
    
token_sales_raw as ( 
    select
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
    contract_address as currency_address,
    
    coalesce (case when event_inputs:to = lower('0xeC1557A67d4980C948cD473075293204F4D280fd')
              then t.event_inputs:value / 1e18 end, 0) as platform_fee_raw,
    coalesce (case when event_inputs:to = seller_address then t.event_inputs:value / 1e18 end , 0) as sale_price_raw,
    coalesce (case when event_inputs:to != seller_address and event_inputs:to != lower('0xeC1557A67d4980C948cD473075293204F4D280fd')
             then t.event_inputs:value / 1e18 end, 0 ) as creator_fee_raw,
    
    f.origin_from_address,
    f.origin_to_address,
    f.origin_function_signature,
    f._log_id,
    f._inserted_timestamp 
    
    FROM {{ ref('silver__logs') }} t 
    
    inner join agg_sales_tx f on t.tx_hash = f.tx_hash 
    where t.block_timestamp >= '2021-01-01'
    
    and t.tx_hash not in (select tx_hash from eth_sales)
    and f.seller_address is not null
    and t.event_inputs:value is not null 
    and nft_address is not null
    and event_name = 'Transfer'

    ),
    
    token_sales as (
    select 
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
    
    sum(platform_fee_raw) as platform_fee,
    sum(sale_price_raw) as sale_price,
    sum(creator_fee_raw) as creator_fee,
    platform_fee + sale_price + creator_fee as price,
    
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    _log_id,
    _inserted_timestamp
    from token_sales_raw 
        
    group by block_number, 
    block_timestamp, 
    tx_hash,
    event_type, 
    platform_address,
    seller_address,
    buyer_address,
    nft_address, 
    tokenId, erc1155_value,
    currency_address, origin_from_address,
    origin_to_address,
    origin_function_signature,
    _log_id,
    _inserted_timestamp
    ),
    
    
    agg_sales as ( 
    
    select* from eth_sales union all 
    select * from token_sales 
        ),
    
    hourly_prices as (
    select 
    hour,
        symbol,
        case when token_address is null and symbol = 'ETH'
            then 'ETH' 
        else token_address end as currency_address,
        price AS token_price
    from {{ ref('silver__prices') }}
    WHERE
        HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                agg_sales_tx
        )
        AND currency_address IN (
            select distinct currency_address from agg_sales 
        )
    ),

    agg_sales_prices as (
    
    select
    block_number, 
    block_timestamp, 
    tx_hash,
    event_type, 
    platform_address, 
    'quixotic' as platform_name, 
    'quixotic' as platform_exchange_version,
    seller_address,
    buyer_address,
    nft_address, 
    tokenId,
    erc1155_value,
    symbol as currency_symbol, 
    s.currency_address,
    price,
    ROUND(
        h.token_price * price,
        2
    ) AS price_usd,

    platform_fee,
    creator_fee,
    platform_fee + creator_fee as total_fees,

    platform_fee * h.token_price as platform_fee_usd, 
    creator_fee * h.token_price as creator_fee_usd, 
    total_fees * h.token_price as total_fees_usd,
    
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    _log_id,
    _inserted_timestamp 
    from agg_sales s 
    
    LEFT JOIN hourly_prices h on h.hour = date_trunc('hour', s.block_timestamp)
                                and h.currency_address = s.currency_address
    
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

    from agg_sales_prices
   
    qualify(ROW_NUMBER() over(PARTITION BY _log_id ORDER BY _inserted_timestamp DESC) = 1)
