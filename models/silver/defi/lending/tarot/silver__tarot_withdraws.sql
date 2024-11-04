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
        token0 as underlying_asset_address, 
        token0_symbol as underlying_asset_symbol,  
        token0_decimals as underlying_decimals
    from 
        {{ ref('silver__tarot_liquidity_pools') }}
    group by all
    UNION ALL
    select 
        borrowable2 as token_address,
        token1 as underlying_asset_address, 
        token1_symbol as underlying_asset_symbol,  
        token1_decimals as underlying_decimals
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
        CONCAT('0x', SUBSTR(topics[2] :: STRING, 25, 40)) AS redeemer,
        utils.udf_hex_to_int(
        segmented_data [0] :: STRING
        ) :: INTEGER AS redeemAmount_raw,
        _inserted_timestamp,
        _log_id
    from 
        {{ ref('core__fact_event_logs') }} l 
    WHERE
        contract_address IN (SELECT TOKEN_ADDRESS FROM asset_details)
    AND
        topics [0] :: STRING = '0x3f693fff038bb8a046aa76d9516190ac7444f7d69cf952c4cbdc086fdef2d6fc'
        AND tx_succeeded
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
    a.token_address,
    'bTAROT' AS token_symbol,
    redeemAmount_raw AS amount_unadj,
    redeemAmount_raw / pow(
        10,
        underlying_decimals
    ) AS amount,
    underlying_asset_address as received_contract_address,
    underlying_asset_symbol as received_contract_symbol,
    redeemer,
    'Tarot' as platform,
    _inserted_timestamp,
    _log_id
FROM
    log_pull l
LEFT JOIN 
    asset_details a
ON
    a.token_address = l.contract_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1