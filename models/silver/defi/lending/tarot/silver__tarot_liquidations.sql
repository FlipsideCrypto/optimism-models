{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

with asset_details as (

    select 
        borrowable1 as token_address,
        token0_name as underlying_asset_address, 
        token0_symbol as underlying_asset_symbol,  
        token0_decimals as underlying_decimals,
        ctarot,
        lp_token_address,
        lp_token_name,
        lp_token_name,
    from 
        {{ ref('silver__tarot_liquidity_pools') }}
    group by all
    UNION ALL
    select 
        borrowable2 as token_address,
        token1_name as underlying_asset_address, 
        token1_symbol as underlying_asset_symbol,  
        token1_decimals as underlying_decimals,
        ctarot
    from 
        {{ ref('silver__tarot_liquidity_pools') }}
    group by all

),
log_pull as (
    select
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics[1] :: STRING, 25, 40)) AS sender,
        CONCAT('0x', SUBSTR(topics[2] :: STRING, 25, 40)) AS borrower,
        CONCAT('0x', SUBSTR(topics[3] :: STRING, 25, 40)) AS liquidator,
        utils.udf_hex_to_int(
        segmented_data [0] :: STRING
        ) :: INTEGER AS seizeTokens_raw,
        utils.udf_hex_to_int(
        segmented_data [1] :: STRING
        ) :: INTEGER AS repayAmount_raw,
        _inserted_timestamp,
        _log_id
    from 
        {{ ref('silver__logs') }} l 
    WHERE
        contract_address IN (SELECT TOKEN_ADDRESS FROM asset_details)
    AND
        topics [0] :: STRING = '0xb0dbe18c6ffdf0da655dd690e77211d379205c497be44c64447c3f5f021b5167'
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
)
select
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    borrower,
    a.ctarot as token,
    'cTAROT' AS token_symbol,
    liquidator,
    seizeTokens_raw / pow(
      10,
      18
    ) AS tokens_seized,
    contract_address as protocol_market,
    asd2.token_symbol AS collateral_token_symbol,
    asd2.underlying_asset_address AS collateral_token,
    asd2.underlying_symbol AS collateral_symbol,
    repayAmount_raw AS amount_unadj,
    repayAmount_raw / pow(
      10,
      a.underlying_decimals
    ) AS amount,
    underlying_asset_address as received_contract_address,
    underlying_asset_symbol as received_contract_symbol,
    'Tarot' as platform,
    _inserted_timestamp,
    _log_id
FROM
    log_pull l
LEFT JOIN 
    asset_details a
ON
    a.token_addres = l.contract_address  qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1