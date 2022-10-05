{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}



-- goal: change all prices to reflect prices + royalty and marketplace fees 
-- issue 1: to account for current optimism contract that accepts nfts 
-- 
-- issue 2: there are erc1155 token sales 
 
with fulfilbasic_three_tx as (
select 
    block_number,
    block_timestamp,
    tx_hash,
    case when origin_from_address = coalesce (event_inputs:_from ::string , event_inputs:from ::string) then 'bid_won'
        else 'sale' end as event_type,
    origin_to_address as platform_address, 
    'quixotic' as platform_name, 
    
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
    
    from OPTIMISM_DEV.silver.logs
    where origin_function_signature in (
     '0xb3a34c4c' 
        ,'0xfb0f3ee1'
        ,'0x87201b41' 
     ) 
    and origin_to_address in ('0x998ef16ea4111094eb5ee72fc2c6f4e6e8647666', '0xc78a09d6a4badecc7614a339fd264b7290361ef1')
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
    'quixotic' as platform_name, 
    
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
    
    from OPTIMISM_DEV.silver.logs
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
     
    from OPTIMISM_DEV.silver.logs
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
    platform_name, 
        
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
    select * from fulfilbasic_three_tx
    union all 
    select * from optimism_campaign_tx),

    hourly_prices as (
    select 
    hour,
        CASE
            WHEN symbol = 'WETH' THEN 'ETH'
            ELSE symbol
        END AS symbol,
        token_address,
        price AS token_price
    from OPTIMISM_DEV.silver.prices
    WHERE
        HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                agg_sales_tx
        )
        AND symbol IN (
            'OP',
            'ETH'
        )
    ),

eth_sales_raw as (  
    select 
    f.block_number, 
    t.block_timestamp, 
    t.tx_hash,
    event_type, 
    platform_address, 
    platform_name, 
    
    seller_address,
    buyer_address,
    nft_address, 
    tokenId,
    erc1155_value,
    'ETH' as currency_symbol, 
    
    coalesce (case when to_address = lower('0xeC1557A67d4980C948cD473075293204F4D280fd')
              then t.eth_value end, 0) as platform_fee_raw,
    case when to_address = seller_address then eth_value end as sale_price_raw,
    coalesce (case when to_address != seller_address and to_address != lower('0xeC1557A67d4980C948cD473075293204F4D280fd')
             then t.eth_value end, 0 ) as creator_fee_raw,
    
    f.origin_from_address,
    f.origin_to_address,
    f.origin_function_signature,
    f._log_id,
    f._inserted_timestamp
        
    from OPTIMISM_DEV.silver.traces t 
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
    platform_name, 
    
    seller_address,
    buyer_address,
    nft_address, 
    tokenId,
    erc1155_value,
    currency_symbol, 
    
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
    platform_name, 
    seller_address,
    buyer_address,
    nft_address, 
    tokenId, erc1155_value,
    currency_symbol, origin_from_address,
    origin_to_address,
    origin_function_signature,
    _log_id,
    _inserted_timestamp
    ),
    
op_sales_raw as ( 
    select
    f.block_number, 
    t.block_timestamp, 
    t.tx_hash,
    event_type, 
    platform_address, 
    platform_name, 
    
    seller_address,
    buyer_address,
    nft_address, 
    tokenId,
    erc1155_value,
    'OP' as currency_symbol, 
    
    coalesce (case when event_inputs:to = lower('0xeC1557A67d4980C948cD473075293204F4D280fd')
              then t.event_inputs:value / 1e18 end, 0) as platform_fee_raw,
    case when event_inputs:to = seller_address then t.event_inputs:value / 1e18 end as sale_price_raw,
    coalesce (case when event_inputs:to != seller_address and event_inputs:to != lower('0xeC1557A67d4980C948cD473075293204F4D280fd')
             then t.event_inputs:value / 1e18 end, 0 ) as creator_fee_raw,
    
    f.origin_from_address,
    f.origin_to_address,
    f.origin_function_signature,
    f._log_id,
    f._inserted_timestamp 
    
    FROM OPTIMISM_DEV.silver.logs t 
    
    inner join agg_sales_tx f on t.tx_hash = f.tx_hash 
    where t.block_timestamp >= '2021-01-01'
    
    and t.tx_hash not in (select tx_hash from eth_sales)
    and f.seller_address is not null
    and nft_address is not null
    and event_name = 'Transfer'
    and contract_address = '0x4200000000000000000000000000000000000042' 
    ),
    
    op_sales as (
    select 
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
    erc1155_value,
    currency_symbol, 
    
    sum(platform_fee_raw) as platform_fee,
    sum(sale_price_raw) as sale_price,
    sum(creator_fee_raw) as creator_fee,
    platform_fee + sale_price + creator_fee as price,
    
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    _log_id,
    _inserted_timestamp
    from op_sales_raw 
        
    group by block_number, 
    block_timestamp, 
    tx_hash,
    event_type, 
    platform_address, 
    platform_name, 
    seller_address,
    buyer_address,
    nft_address, 
    tokenId, erc1155_value,
    currency_symbol, origin_from_address,
    origin_to_address,
    origin_function_signature,
    _log_id,
    _inserted_timestamp
    ),
    
other_eth_raw as ( 
    select
    f.block_number, 
    t.block_timestamp, 
    t.tx_hash,
    event_type, 
    platform_address, 
    platform_name, 
    
    seller_address,
    buyer_address,
    nft_address, 
    tokenId,
    erc1155_value,
    'ETH' as currency_symbol, 
    
    coalesce (case when event_inputs:to = lower('0xeC1557A67d4980C948cD473075293204F4D280fd')
              then t.event_inputs:value / 1e18 end, 0) as platform_fee_raw,
    case when event_inputs:to = seller_address then t.event_inputs:value / 1e18 end as sale_price_raw,
    coalesce (case when event_inputs:to != seller_address and event_inputs:to != lower('0xeC1557A67d4980C948cD473075293204F4D280fd')
             then t.event_inputs:value / 1e18 end, 0 ) as creator_fee_raw,
    
    f.origin_from_address,
    f.origin_to_address,
    f.origin_function_signature,
    f._log_id,
    f._inserted_timestamp 
    
    FROM OPTIMISM_DEV.silver.logs t 
    
    inner join agg_sales_tx f on t.tx_hash = f.tx_hash 
    where t.block_timestamp >= '2021-01-01'
    
    and t.tx_hash not in (select tx_hash from eth_sales)
    and t.tx_hash not in (select tx_hash from op_sales)
    and f.seller_address is not null
    and nft_address is not null
    and event_name = 'Transfer'
    and contract_address = '0x4200000000000000000000000000000000000006'  
    ),
    
    other_eth_sales as (
    select 
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
    erc1155_value,
    currency_symbol, 
    
    sum(platform_fee_raw) as platform_fee,
    sum(sale_price_raw) as sale_price,
    sum(creator_fee_raw) as creator_fee,
    platform_fee + sale_price + creator_fee as price,
    
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    _log_id,
    _inserted_timestamp
    from other_eth_raw 
        
    group by block_number, 
    block_timestamp, 
    tx_hash,
    event_type, 
    platform_address, 
    platform_name, 
    seller_address,
    buyer_address,
    nft_address, 
    tokenId, erc1155_value,
    currency_symbol, origin_from_address,
    origin_to_address,
    origin_function_signature,
    _log_id,
    _inserted_timestamp
    ),
    
    
    agg_sales as ( 
    
    select* from eth_sales union all 
    select * from op_sales union all 
    select * from other_eth_sales
        ) 
        
    select
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
    erc1155_value,
    currency_symbol, 
    CASE
        WHEN currency_symbol = 'ETH' THEN 'ETH'
        WHEN currency_symbol = 'OP' THEN token_address
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
    from agg_sales 
    
    LEFT JOIN hourly_prices
    ON DATE_TRUNC(
        'HOUR',
        block_timestamp
    ) = HOUR
    AND symbol = currency_symbol
   
    qualify(ROW_NUMBER() over(PARTITION BY _log_id ORDER BY _inserted_timestamp DESC) = 1)
    
    