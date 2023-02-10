{{ config(
    materialized = 'incremental',
    unique_key = "_log_id",
    cluster_by = ['block_timestamp::DATE']
) }}

{{ config(
    materialized = 'incremental',
    unique_key = "_log_id",
    cluster_by = ['block_timestamp::DATE']
) }}

WITH rubicon_base AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        event_name,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS sender,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x',SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS token_in,
        CONCAT('0x',SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS token_out,
        CONCAT('0x',SUBSTR(segmented_data [5] :: STRING, 25, 40)) AS recipient,
        ethereum.public.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS tokens_sold,
        ethereum.public.udf_hex_to_int(
            segmented_data [4] :: STRING
        ) :: INTEGER AS tokens_bought,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }} a
    WHERE
        topics [0] :: STRING IN (
            '0xe246f5c6615e4a819ae6062dd22d99f289cc78afbf91469c64a81584c45475e5'
        ) 
)

SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    tx_hash,
    event_index,
    event_name,
    sender,
    recipient AS tx_to,
    token_in,
    token_out,
    c0.symbol AS symbol_in,
    c1.symbol AS symbol_out,
    CONCAT(symbol_in,'-',symbol_out) AS pool_name,
    c0.decimals AS decimals_in,
    c1.decimals AS decimals_out,
    tokens_sold AS amount_in,
    tokens_bought AS amount_out,
    CASE
        WHEN decimals_in IS NOT NULL THEN tokens_sold / pow(10,decimals_in)
        ELSE tokens_sold
    END AS amount_in_adj,
    CASE
        WHEN decimals_out IS NOT NULL THEN tokens_bought / pow(10,decimals_out)
        ELSE tokens_bought
    END AS amount_out_adj,
    CASE
        WHEN decimals_in IS NOT NULL THEN ROUND(amount_in_adj * p0.price,2)
    END AS amount_in_usd,
    CASE
        WHEN decimals_out IS NOT NULL THEN ROUND(amount_out_adj * p1.price,2)
    END AS amount_out_usd,
    _log_id,
    r._inserted_timestamp
FROM rubicon_base r
LEFT JOIN {{ ref('silver__prices') }} p0
    ON p0.hour = DATE_TRUNC(
        'hour',
        block_timestamp
    )
    AND p0.token_address = r.token_in
LEFT JOIN {{ ref('silver__prices') }} p1
    ON p1.hour = DATE_TRUNC(
        'hour',
        block_timestamp
    )
    AND p1.token_address = r.token_out
LEFT JOIN {{ ref('core__dim_contracts') }} c0
    ON c0.address = r.token_in
LEFT JOIN {{ ref('core__dim_contracts') }} c1
        ON c1.address = r.token_out