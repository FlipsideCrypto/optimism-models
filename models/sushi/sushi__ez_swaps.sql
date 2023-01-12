{{ config(
    materialized = 'incremental',
    persist_docs ={ "relation": true,
    "columns": true },
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH swap_events AS (

    SELECT
        block_number,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        block_timestamp,
        tx_hash,
        contract_address,
        event_name,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            public.udf_hex_to_int(segmented_data[0]::string)::integer
        ) AS amountIn,
        TRY_TO_NUMBER(
            public.udf_hex_to_int(segmented_data[1]::string)::integer
        ) AS amountOut,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS token_out, 
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token_in,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS tx_to,
        event_index,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics[0]::string = '0xcd3829a3813dc3cdd188fd3d01dcf3268c16be2fdd2dd21d0665418816e46062'
        AND tx_status = 'SUCCESS'
        AND contract_address IN (
            SELECT
                DISTINCT pool_address
            FROM
                {{ ref('sushi__dim_dex_pools') }}
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        contract_address,
        event_name,
        case when token_in = token0_address then amountIn/power( 10, token0_decimals) :: FLOAT 
             when token_in = token1_address then amountIn/power( 10, token1_decimals) :: FLOAT
             END AS amount_in,
        case when token_out = token0_address then amountOut/power( 10, token0_decimals) :: FLOAT 
             when token_out = token1_address then amountOut/power( 10, token1_decimals) :: FLOAT
             END AS amount_out,
        tx_to,
        event_index,
        _log_id,
        CASE when token_in = token0_address then token0_symbol
             when token_in = token1_address then token1_symbol
        END AS symbol_in,
        CASE when token_out = token0_address then token0_symbol
             when token_out = token1_address then token1_symbol
        END AS symbol_out,
        token_in,
        token_out,
        pool_name,
        _inserted_timestamp
    FROM
        swap_events a
        LEFT JOIN {{ ref('sushi__dim_dex_pools') }} bb
        ON a.contract_address = bb.pool_address 
),

 optimism_prices AS (
    select 
        symbol,
        date_trunc('hour',recorded_at) as hour, 
        avg(price) as price 
    from 
        {{ source('prices','prices_v2') }} 
    where symbol in (select token0_symbol as symbol from {{ ref('sushi__dim_dex_pools') }}
                     union all 
                     select token1_symbol as symbol from {{ ref('sushi__dim_dex_pools') }})

{% if is_incremental() %}
AND hour :: DATE IN (
    SELECT
        DISTINCT block_timestamp :: DATE
    FROM
        swap_events
)
{% else %}
    AND hour :: DATE >= '2021-09-01'
{% endif %}
    group by 1,2
)

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    'sushiswap' AS platform,
    pool_name,
    amount_in,
    case
    WHEN amount_in * pIn.price <= 5 * amount_out * pOut.price
    AND amount_out * pOut.price <= 5 * amount_in * pIn.price THEN amount_in * pIn.price
    when pOut.price is null then amount_in * pIn.price
    ELSE NULL
    END AS amount_in_usd,
    amount_out,
    CASE
    WHEN amount_in * pIn.price <= 5 * amount_out * pOut.price
    AND amount_out * pOut.price <= 5 * amount_in * pIn.price THEN amount_out * pOut.price
    when pIn.price is null then amount_out * pOut.price
    ELSE NULL
    END AS amount_out_usd,
    tx_to,
    event_index,
    event_name,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    _log_id,
    _inserted_timestamp
FROM
    FINAL wp
    LEFT JOIN optimism_prices pIn
    ON lower(wp.symbol_in) = lower(pIn.symbol)
    AND DATE_TRUNC(
        'hour',
        wp.block_timestamp
    ) = pIn.hour
    LEFT JOIN optimism_prices pOut
    ON lower(wp.symbol_out) = lower(pOut.symbol)
    AND DATE_TRUNC(
        'hour',
        wp.block_timestamp
    ) = pOut.hour
