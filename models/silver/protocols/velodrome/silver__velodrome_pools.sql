{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    post_hook = "{{ fsc_utils.block_reorg(this, 12) }}",
    tags = ['non_realtime'],
    full_refresh = false
) }}

WITH pool_creation AS (

    SELECT
        block_timestamp AS created_timestamp,
        block_number,
        tx_hash AS created_hash,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS token0_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token1_address,
        CONCAT('0x', SUBSTR(DATA :: STRING, 91, 40)) AS pool_address,
        CASE
            WHEN SUBSTR(
                DATA,
                66,
                1
            ) = 1 THEN 'stable'
            ELSE 'volatile'
        END AS pool_type,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xc4805696c66d7cf352fc1d6bb633ad5ee82f6cb577c453024b6e0eb8306c6fc9' -- pair created
        AND contract_address = '0x25cbddb98b35ab1ff77413456b31ec81a6b6b746' --velo deployer
{% if is_incremental() %}
AND pool_address NOT IN (
    SELECT
        DISTINCT pool_address
    FROM
        {{ this }}
)
{% endif %}
),

function_sigs AS (

SELECT
    '0x06fdde03' AS function_sig, 
    'name' AS function_name
UNION ALL
SELECT
    '0x95d89b41' AS function_sig, 
    'symbol' AS function_name
UNION ALL
SELECT
    '0x313ce567' AS function_sig, 
    'decimals' AS function_name
),

all_inputs AS (

SELECT
    pool_address AS contract_address,
    block_number,
    function_sig,
    (ROW_NUMBER() OVER (PARTITION BY pool_address
        ORDER BY block_number)) - 1 AS function_input,
    'pool' AS address_label
FROM pool_creation
JOIN function_sigs ON 1=1 
UNION ALL
SELECT
    token0_address AS contract_address,
    block_number,
    function_sig,
    (ROW_NUMBER() OVER (PARTITION BY token0_address
        ORDER BY block_number)) - 1 AS function_input,
    'token0' AS address_label
FROM pool_creation
JOIN function_sigs ON 1=1
UNION ALL
SELECT
    token1_address AS contract_address,
    block_number,
    function_sig,
    (ROW_NUMBER() OVER (PARTITION BY token1_address
        ORDER BY block_number)) - 1 AS function_input,
    'token1' AS address_label
FROM pool_creation
JOIN function_sigs ON 1=1 
),

ready_reads_all AS (
SELECT 
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
        function_input,
        '\']'
        ) AS read_input
FROM all_inputs
),

batch_reads_all AS (
    
SELECT
    CONCAT('[', LISTAGG(read_input, ','), ']') AS batch_read
FROM
    ready_reads_all
),

all_reads AS (

SELECT
    ethereum.streamline.udf_json_rpc_read_calls(
        node_url,
        headers,
        PARSE_JSON(batch_read)
    ) AS read_output,
    SYSDATE() AS _inserted_timestamp
FROM
    batch_reads_all
JOIN streamline.crosschain.node_mapping ON 1=1 
    AND chain = 'optimism'
WHERE
    EXISTS (
        SELECT
            1
        FROM
            ready_reads_all
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
    all_reads,
    LATERAL FLATTEN(
        input => read_output [0] :data
    ) 
),

details AS (

SELECT
    contract_address,
    function_sig,
    function_name,
    read_result,
    regexp_substr_all(SUBSTR(read_result, 3, len(read_result)), '.{64}') AS segmented_output,
    _inserted_timestamp
FROM reads_adjusted
LEFT JOIN function_sigs USING(function_sig)
    ),

pools AS (
    
SELECT
    d.contract_address AS pool_address,
    MIN(CASE WHEN function_name = 'symbol' THEN utils.udf_hex_to_string(segmented_output [2] :: STRING) END) AS pool_symbol,
    MIN(CASE WHEN function_name = 'name' THEN utils.udf_hex_to_string(segmented_output [2] :: STRING) END) AS pool_name,
    MIN(CASE 
            WHEN read_result::STRING = '0x' THEN NULL
            ELSE utils.udf_hex_to_int(read_result::STRING)
        END)::INTEGER  AS pool_decimals,
    MAX(_inserted_timestamp) AS _inserted_timestamp
FROM details d
LEFT JOIN all_inputs i ON d.contract_address = i.contract_address
WHERE address_label = 'pool'
GROUP BY 1
),

token0 AS (

SELECT
    d.contract_address AS token0_address,
    MIN(CASE WHEN function_name = 'symbol' THEN utils.udf_hex_to_string(segmented_output [2] :: STRING) END) AS token0_symbol,
    MIN(CASE WHEN function_name = 'name' THEN utils.udf_hex_to_string(segmented_output [2] :: STRING) END) AS token0_name,
    MIN(CASE 
            WHEN function_name = 'decimals' AND read_result::STRING <> '0x' THEN utils.udf_hex_to_int(segmented_output [0] :: STRING)
            ELSE NULL
        END)::INTEGER AS token0_decimals,
    MAX(_inserted_timestamp) AS _inserted_timestamp
FROM details d
LEFT JOIN all_inputs i ON d.contract_address = i.contract_address
WHERE address_label = 'token0'
GROUP BY 1
),

token1 AS (

SELECT
    d.contract_address AS token1_address,
    MIN(CASE WHEN function_name = 'symbol' THEN utils.udf_hex_to_string(segmented_output [2] :: STRING) END) AS token1_symbol,
    MIN(CASE WHEN function_name = 'name' THEN utils.udf_hex_to_string(segmented_output [2] :: STRING) END) AS token1_name,
    MIN(CASE 
            WHEN function_name = 'decimals' AND read_result::STRING <> '0x' THEN utils.udf_hex_to_int(segmented_output [0] :: STRING)
            ELSE NULL
        END)::INTEGER AS token1_decimals,
    MAX(_inserted_timestamp) AS _inserted_timestamp
FROM details d
LEFT JOIN all_inputs i ON d.contract_address = i.contract_address
WHERE address_label = 'token1'
GROUP BY 1
)

SELECT
    pool_address,
    pool_name,
    pool_type,
    REGEXP_REPLACE(pool_symbol,'[^a-zA-Z0-9/-]+') AS pool_symbol,
    pool_decimals,
    REGEXP_REPLACE(token0_symbol,'[^a-zA-Z0-9]+') AS token0_symbol,
    REGEXP_REPLACE(token1_symbol,'[^a-zA-Z0-9]+') AS token1_symbol,
    c.token0_address,
    c.token1_address,
    token0_decimals,
    token1_decimals,
    created_timestamp,
    block_number AS created_block,
    created_hash,
    a._inserted_timestamp
FROM pools a
LEFT JOIN pool_creation c USING(pool_address)
LEFT JOIN token0 ON c.token0_address = token0.token0_address
LEFT JOIN token1 ON c.token1_address = token1.token1_address
