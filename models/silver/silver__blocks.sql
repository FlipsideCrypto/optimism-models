{{ config(
    materialized = 'incremental',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE']
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
        ingested_at,
        _inserted_timestamp
    FROM
        {{ ref('bronze__blocks') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
),
all_records AS (
    SELECT
        block_id :: INTEGER AS block_number,
        block_timestamp :: TIMESTAMP AS block_timestamp,
        network :: STRING AS network,
        chain_id :: STRING AS blockchain,
        tx_count :: INTEGER AS tx_count,
        ethereum.public.udf_hex_to_int(
            header :difficulty :: STRING
        ) :: INTEGER AS difficulty,
        ethereum.public.udf_hex_to_int(
            header :totalDifficulty :: STRING
        ) :: INTEGER AS total_difficulty,
        header: extraData :: STRING AS extra_data,
        ethereum.public.udf_hex_to_int(
            header :gasLimit :: STRING
        ) :: INTEGER AS gas_limit,
        ethereum.public.udf_hex_to_int(
            header :gasUsed :: STRING
        ) :: INTEGER AS gas_used,
        header: "hash" :: STRING AS HASH,
        header: parentHash :: STRING AS parent_hash,
        header: receiptsRoot :: STRING AS receipts_root,
        header: sha3Uncles :: STRING AS sha3_uncles,
        ethereum.public.udf_hex_to_int(
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
        header :: OBJECT AS block_header_json,
        base_tables._inserted_timestamp :: TIMESTAMP AS _inserted_timestamp
    FROM
        base_tables
)
SELECT
    block_number,
    block_timestamp,
    network,
    blockchain,
    tx_count,
    difficulty,
    total_difficulty,
    extra_data,
    gas_limit,
    gas_used,
    HASH,
    parent_hash,
    receipts_root,
    sha3_uncles,
    SIZE,
    uncle_blocks,
    ingested_at,
    block_header_json,
    all_records._inserted_timestamp AS _inserted_timestamp
FROM
    all_records qualify(ROW_NUMBER() over(PARTITION BY block_number
ORDER BY
    all_records._inserted_timestamp DESC)) = 1
