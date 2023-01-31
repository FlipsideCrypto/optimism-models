{{ config(
    materialized = 'incremental',
    unique_key = "pool_id"
) }}

WITH contract_deployments AS (

SELECT
    a.tx_hash AS tx_hash,
    a.block_number AS block_number,
    a.block_timestamp AS block_timestamp,
    a.from_address AS deployer_address,
    a.to_address AS contract_address,
    a._inserted_timestamp AS _inserted_timestamp
FROM
    {{ ref('silver__traces' )}} a
WHERE 
    -- curve contract deployers
    a.from_address IN (
        '0x2db0e83599a91b508ac268a6197b8b14f5e72840',
        '0x7eeac6cddbd1d0b8af061742d41877d7f707289a',
        '0x745748bcfd8f9c2de519a71d789be8a63dd7d66c')
    AND TYPE ilike 'create%'
    AND TX_STATUS ilike 'success'
{% if is_incremental() %}
AND a.to_address NOT IN (
    SELECT
        DISTINCT pool_address
    FROM
        {{ this }}
)
{% endif %}
QUALIFY(ROW_NUMBER() OVER(PARTITION BY a.to_address ORDER BY block_timestamp ASC)) = 1
),

function_sigs AS (

SELECT
    LEFT(udfs_dev.streamline.udf_hex_encode_function('coins(uint256)')::STRING,10) AS function_sig, 
    'coins' AS function_name
UNION ALL
SELECT
    LEFT(udfs_dev.streamline.udf_hex_encode_function('name()')::STRING,10) AS function_sig, 
    'name' AS function_name
UNION ALL
SELECT
    LEFT(udfs_dev.streamline.udf_hex_encode_function('symbol()')::STRING,10) AS function_sig, 
    'symbol' AS function_name
UNION ALL
SELECT
    LEFT(udfs_dev.streamline.udf_hex_encode_function('decimals()')::STRING,10) AS function_sig, 
    'decimals' AS function_name
),

function_inputs AS (
    SELECT
        SEQ4() AS function_input
    FROM
        TABLE(GENERATOR(rowcount => 4))
),

inputs_coins AS (

SELECT
    deployer_address,
    contract_address,
    block_number,
    function_sig,
    (ROW_NUMBER() OVER (PARTITION BY contract_address
        ORDER BY block_number)) - 1 AS function_input
FROM contract_deployments
JOIN function_sigs ON 1=1
JOIN function_inputs ON 1=1
    WHERE function_name = 'coins'
),

inputs_pool_details AS (

SELECT
    deployer_address,
    contract_address,
    block_number,
    function_sig,
    NULL AS function_input
FROM contract_deployments
JOIN function_sigs ON 1=1
WHERE function_name IN ('name','symbol','decimals')
),

all_inputs AS (

SELECT
    deployer_address,
    contract_address,
    block_number,
    function_sig,
    function_input
FROM inputs_coins
UNION ALL
SELECT
    deployer_address,
    contract_address,
    block_number,
    function_sig,
    function_input
FROM inputs_pool_details
),

ready_reads_pools AS (
SELECT 
    deployer_address,
    contract_address,
    block_number,
    function_sig,
    function_input,
    CONCAT(
        '[\'',
        contract_address,
        '\',',
        block_number,
        ',\'',
        function_sig,
        '\',\'',
        (CASE WHEN function_input IS NULL THEN '' ELSE function_input::STRING END),
        '\']'
        ) AS read_input
FROM all_inputs
),

batch_reads_pools AS (
    
SELECT
    CONCAT('[', LISTAGG(read_input, ','), ']') AS batch_read
FROM
    ready_reads_pools
),

pool_token_reads AS (

SELECT
    ethereum.streamline.udf_json_rpc_read_calls(
        node_url,
        headers,
        PARSE_JSON(batch_read)
    ) AS read_output,
    SYSDATE() AS _inserted_timestamp
FROM
    batch_reads_pools
JOIN streamline.crosschain.node_mapping ON 1=1 
    AND chain = 'optimism'
WHERE
    EXISTS (
        SELECT
            1
        FROM
            ready_reads_pools
        LIMIT
            1
    ) 
),

reads_adjusted AS (
    
SELECT
        VALUE :id :: STRING AS read_id,
        VALUE :result :: STRING AS read_result,
        SPLIT(
            read_id,
            '-'
        ) AS read_id_object,
        read_id_object [0] :: STRING AS contract_address,
        read_id_object [1] :: STRING AS block_number,
        read_id_object [2] :: STRING AS function_sig,
        read_id_object [3] :: STRING AS function_input,
        _inserted_timestamp
FROM
    pool_token_reads,
    LATERAL FLATTEN(
        input => read_output [0] :data
    ) 
),

tokens AS (
    
SELECT
    contract_address,
    function_sig,
    function_name,
    read_result,
    regexp_substr_all(SUBSTR(read_result, 3, len(read_result)), '.{64}')[0]AS segmented_token_address,
    _inserted_timestamp
FROM reads_adjusted
LEFT JOIN function_sigs USING(function_sig)
WHERE read_result IS NOT NULL
    AND function_name = 'coins'
),

pool_details AS (

SELECT
    contract_address,
    function_sig,
    function_name,
    read_result,
    regexp_substr_all(SUBSTR(read_result, 3, len(read_result)), '.{64}') AS segmented_output,
    _inserted_timestamp
FROM reads_adjusted
LEFT JOIN function_sigs USING(function_sig)
WHERE read_result IS NOT NULL
    AND function_name IN ('name','symbol','decimals')
),

FINAL AS (
SELECT
    t.contract_address AS pool_address,
    CONCAT('0x',SUBSTRING(t.segmented_token_address,25,40)) AS token_address,
    MIN(CASE WHEN p.function_name = 'symbol' THEN TRY_HEX_DECODE_STRING(RTRIM(p.segmented_output [2] :: STRING, 0)) END) AS pool_symbol,
    MIN(CASE WHEN p.function_name = 'name' THEN CONCAT(TRY_HEX_DECODE_STRING(p.segmented_output [2] :: STRING),
        TRY_HEX_DECODE_STRING(segmented_output [3] :: STRING)) END) AS pool_name,
    MIN(CASE 
            WHEN p.read_result::STRING = '0x' THEN NULL
            ELSE udf_hex_to_int(p.read_result::STRING)
        END)::INTEGER  AS pool_decimals,
    CONCAT(
        t.contract_address,
        '-',
        CONCAT('0x',SUBSTRING(t.segmented_token_address,25,40))
    ) AS pool_id,
    MAX(t._inserted_timestamp) AS _inserted_timestamp
FROM tokens t
LEFT JOIN pool_details p ON t.contract_address = p.contract_address
WHERE token_address IS NOT NULL 
    AND token_address <> '0x0000000000000000000000000000000000000000'
GROUP BY 1,2
)

SELECT
    pool_address,
    CASE
        WHEN token_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0x4200000000000000000000000000000000000006'
        ELSE token_address
    END AS token_address,
    CASE 
        WHEN token_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 'WETH'
        ELSE pool_symbol
    END AS pool_symbol,
    pool_name,
    pool_decimals,
    pool_id,
    _inserted_timestamp
FROM FINAL