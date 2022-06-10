{{ config(
    materialized = 'incremental',
    unique_key = "tx_hash",
    cluster_by = ['ingested_at::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH base_table AS (

    SELECT
        block_timestamp,
        block_id :: INTEGER AS block_number,
        tx_id :: STRING AS tx_hash,
        udf_hex_to_int(
            tx :nonce :: STRING
        ) :: INTEGER AS nonce,
        tx_block_index :: INTEGER AS POSITION,
        tx :from :: STRING AS from_address,
        tx :to :: STRING AS to_address,
        (
            udf_hex_to_int(
                tx :value :: STRING
            ) / pow(
                10,
                18
            )
        ) :: FLOAT AS eth_value,
        tx :blockHash :: STRING AS block_hash,
        (
            udf_hex_to_int(
                tx :gasPrice :: STRING
            ) / pow(
                10,
                9
            )
        ) :: FLOAT AS gas_price,
        udf_hex_to_int(
            tx :gas :: STRING
        ) :: INTEGER AS gas_limit,
        tx :input :: STRING AS DATA,
        CASE
            WHEN tx :receipt :status :: STRING = '0x1' THEN 'SUCCESS'
            ELSE 'FAIL'
        END AS status,
        udf_hex_to_int(
            tx :receipt :gasUsed :: STRING
        ) :: INTEGER AS gas_used,
        udf_hex_to_int(
            tx :receipt :cumulativeGasUsed :: STRING
        ) :: INTEGER AS cumulative_Gas_Used,
        udf_hex_to_int(
            tx :receipt :effectiveGasPrice :: STRING
        ) :: INTEGER AS effective_Gas_Price,
        (gas_price * gas_used) / pow(10,9) As tx_fee,
        ingested_at :: TIMESTAMP AS ingested_at,
        OBJECT_DELETE(
            tx,
            'traces'
        ) AS tx_json
    FROM
        {{ ref('bronze__transactions') }}

{% if is_incremental() %}
WHERE
    ingested_at >= (
        SELECT
            MAX(
                ingested_at
            )
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    block_timestamp,
    block_number,
    tx_hash,
    nonce,
    POSITION,
    SUBSTR(
        DATA,
        1,
        10
    ) AS origin_function_signature,
    from_address,
    to_address,
    eth_value,
    block_hash,
    gas_price,
    gas_limit,
    DATA AS input_data,
    status,
    gas_used,
    cumulative_Gas_Used,
    effective_Gas_Price,
    tx_fee,
    ingested_at,
    tx_json
FROM
    base_table qualify(ROW_NUMBER() over(PARTITION BY tx_hash
ORDER BY
    ingested_at DESC)) = 1
