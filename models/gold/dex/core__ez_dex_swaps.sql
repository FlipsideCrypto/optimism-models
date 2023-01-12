{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

WITH velodrome_swaps AS (

  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in,
    amount_in_usd,
    amount_out,
    amount_out_usd,
    sender_address AS sender,
    to_address AS tx_to,
    event_index,
    platform,
    token_address_in AS token_in,
    token_address_out AS token_out,
    symbol_in,
    symbol_out,
    _log_id
  FROM
    {{ ref('velodrome__ez_swaps') }}
),
sushi_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in,
    amount_in_usd,
    amount_out,
    amount_out_usd,
    origin_from_address AS sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    _log_id
  FROM
    {{ ref('sushi__ez_swaps') }}
),
synthetix_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in,
    amount_in_usd,
    amount_out,
    amount_out_usd,
    sender,
    tx_to,
    event_index,
    platform,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    _log_id
  FROM
    {{ ref('silver_dex__synthetix_swaps') }}
)

SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_in_usd,
  amount_out,
  amount_out_usd,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  _log_id
FROM
  velodrome_swaps
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_in_usd,
  amount_out,
  amount_out_usd,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  _log_id
FROM
  sushi_swaps
UNION ALL
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in,
  amount_in_usd,
  amount_out,
  amount_out_usd,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  _log_id
FROM
  synthetix_swaps
