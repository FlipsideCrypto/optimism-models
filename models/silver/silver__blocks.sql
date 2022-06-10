{{ config(
    materialized = 'incremental',
    unique_key = "block_number",
    cluster_by = ['ingested_at::DATE']
) }}

WITH base_tables AS (

    SELECT
        record_id,
        offset_id,
        block_id,
        block_timestamp,
        network,
        chain_id,
        tx_count,
        header,
        ingested_at
    FROM
        {{ ref('bronze__blocks') }}

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
    block_id :: INTEGER AS block_number,
    block_timestamp :: TIMESTAMP AS block_timestamp,
    network :: STRING AS network,
    chain_id :: STRING AS blockchain,
    tx_count :: INTEGER AS tx_count,
    udf_hex_to_int(
        header :difficulty :: STRING
    ) :: INTEGER AS difficulty,
    udf_hex_to_int(
        header :totalDifficulty :: STRING
    ) :: INTEGER AS total_difficulty,
    header: extraData :: STRING AS extra_data,
    udf_hex_to_int(
        header :gasLimit :: STRING
    ) :: INTEGER AS gas_limit,
    udf_hex_to_int(
        header :gasUsed :: STRING
    ) :: INTEGER AS gas_used,
    header: "hash" :: STRING AS HASH,
    header: parentHash :: STRING AS parent_hash,
    header: miner :: STRING AS miner,
    header: nonce :: STRING AS nonce,
    header: receiptsRoot :: STRING AS receipts_root,
    header: sha3Uncles :: STRING AS sha3_uncles,
    udf_hex_to_int(
        header: "size" :: STRING
    ) :: INTEGER AS SIZE,
    CASE
        WHEN header: uncles [1] :: STRING IS NOT NULL THEN CONCAT(
            header: uncles [0] :: STRING,
            ', ',
            header: uncles [1] :: STRING
        )
        ELSE header: uncles [0] :: STRING
    END AS uncle_blocks,
    ingested_at :: TIMESTAMP AS ingested_at,
    header :: OBJECT AS block_header_json
FROM
    base_tables qualify(ROW_NUMBER() over(PARTITION BY block_id
ORDER BY
    ingested_at DESC)) = 1
