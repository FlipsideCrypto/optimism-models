{{ config(
    materialized = 'incremental',
    unique_key = "_log_id",
    cluster_by = ['block_timestamp::DATE']
) }}

WITH pool_meta AS (

    SELECT
        DISTINCT pool_address,
        CASE 
            WHEN pool_name IS NULL AND pool_symbol IS NULL THEN CONCAT('Curve.fi Pool: ',SUBSTRING(pool_address, 1, 5),'...',SUBSTRING(pool_address, 39, 42))
            WHEN pool_name IS NULL THEN CONCAT('Curve.fi Pool: ',pool_symbol)
            ELSE pool_name
        END AS pool_name
    FROM
        {{ ref('silver_dex__curve_pools') }}
),

curve_base AS (
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
        contract_address AS pool_address,
        pool_name,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS sender,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS recipient,
        ethereum.public.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS sold_id,
        ethereum.public.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS tokens_sold,
        ethereum.public.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: INTEGER AS bought_id,
        ethereum.public.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INTEGER AS tokens_bought,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        INNER JOIN pool_meta
        ON pool_meta.pool_address = contract_address
    WHERE
        topics [0] :: STRING IN (
            '0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140',
            '0xd013ca23e77a65003c2c659c5442c00c805371b7fc1ebd4c206c41d1536bd90b'
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
token_transfers AS (
    SELECT
        tx_hash,
        contract_address AS token_address,
        TRY_TO_NUMBER(
            CASE
                WHEN DATA :: STRING = '0x' THEN NULL
                ELSE ethereum.public.udf_hex_to_int(
                    DATA :: STRING
            ) END ) AS amount,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS from_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS to_address
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                curve_base
        )
        AND CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) <> '0x0000000000000000000000000000000000000000'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
from_transfers AS (
    SELECT
        DISTINCT tx_hash,
        token_address,
        from_address,
        amount
    FROM
        token_transfers
),
to_transfers AS (
    SELECT
        DISTINCT tx_hash,
        token_address,
        to_address,
        amount
    FROM
        token_transfers
),
pool_info AS (
    SELECT
        s.block_number,
        s.block_timestamp,
        s.tx_hash,
        s.origin_function_signature,
        s.origin_from_address,
        s.origin_to_address,
        CASE 
            WHEN s.recipient IS NULL THEN s.origin_from_address 
            ELSE s.recipient 
        END AS tx_to,
        event_index,
        event_name,
        pool_address,
        pool_address AS contract_address,
        pool_name,
        sender,
        sold_id,
        tokens_sold,
        sold.token_address AS token_in,
        bought_id,
        tokens_bought,
        bought.token_address AS token_out,
        _log_id,
        _inserted_timestamp
    FROM
        curve_base s
        LEFT JOIN from_transfers sold
        ON tokens_sold = sold.amount
        AND s.tx_hash = sold.tx_hash 
        LEFT JOIN to_transfers bought
        ON tokens_bought = bought.amount
        AND s.tx_hash = bought.tx_hash 
    WHERE
        tokens_sold <> 0 
qualify(ROW_NUMBER() over(PARTITION BY _log_id
    ORDER BY _inserted_timestamp DESC)) = 1
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
    tx_to,
    pool_address,
    pool_name,
    sender,
    token_in,
    token_out,
    tokens_sold,
    tokens_bought,
    _log_id,
    _inserted_timestamp,
    'curve' AS platform
FROM
    pool_info
