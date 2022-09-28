{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

with fulfilbasic_three_tx as (
select 
    block_number,
    block_timestamp,
    tx_hash,
    'sale' as event_type, 
    origin_to_address as platform_address, 
    'quixotic' as platform_name, 
    
    event_inputs:from :: string as seller_address,
    event_inputs:to :: string as buyer_address,
    contract_address as nft_address, 
    event_inputs:tokenId :: string as tokenId,
     
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
     ) 
    and origin_to_address in ('0x998ef16ea4111094eb5ee72fc2c6f4e6e8647666', '0xc78a09d6a4badecc7614a339fd264b7290361ef1')
    and block_timestamp >= '2021-01-01' 
    and event_name = 'Transfer'
    and event_inputs:tokenId is not null 
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

    hourly_prices as (
    select 
    hour,
        CASE
            WHEN symbol = 'WETH' THEN 'ETH'
            ELSE symbol
        END AS symbol,
        token_address,
        price AS token_price
    from {{ ref('silver__prices') }}
    WHERE
        HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                fulfilbasic_three_tx
        )
        AND symbol IN (
            'OP',
            'WETH'
        )
    ),

eth_sales as (  
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
    'ETH' as currency_symbol, 
    t.eth_value as price,
    
    f.origin_from_address,
    f.origin_to_address,
    f.origin_function_signature,
    f._log_id,
    f._inserted_timestamp
        
    from OPTIMISM.SILVER.TRACES t 
    inner join fulfilbasic_three_tx f on t.tx_hash = f.tx_hash and t.to_address = f.seller_address 
    where t.block_timestamp >= '2021-01-01'
    and seller_address is not null 
    and nft_address is not null 
    and t.eth_value > 0 
    and identifier != 'CALL_ORIGIN'
    ),
    
op_sales as ( 
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
    'OP' as currency_symbol, 
    t.event_inputs:value / 1e18 as price,
    
    f.origin_from_address,
    f.origin_to_address,
    f.origin_function_signature,
    f._log_id,
    f._inserted_timestamp 
    
    FROM optimism.silver.logs t 
    
    inner join fulfilbasic_three_tx f on t.tx_hash = f.tx_hash and f.seller_address = t.event_inputs:to 
    where t.block_timestamp >= '2021-01-01'
    
    and t.tx_hash not in (select tx_hash from eth_sales)
    and f.seller_address is not null
    and nft_address is not null
    and event_name = 'Transfer'
    and contract_address = '0x4200000000000000000000000000000000000042' 
    ),

other_eth_sales as ( 
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
    'ETH' as currency_symbol, 
    t.event_inputs:value / 1e18 as price,
    
    f.origin_from_address,
    f.origin_to_address,
    f.origin_function_signature,
    f._log_id,
    f._inserted_timestamp 
    
    FROM optimism.silver.logs t 
    
    inner join fulfilbasic_three_tx f on t.tx_hash = f.tx_hash and f.seller_address = t.event_inputs:to 
    where t.block_timestamp >= '2021-01-01'
    
    and t.tx_hash not in (select tx_hash from eth_sales)
    and t.tx_hash not in (select tx_hash from op_sales)
    and f.seller_address is not null
    and nft_address is not null
    and event_name = 'Transfer'
    and contract_address = '0x4200000000000000000000000000000000000006'  
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
   