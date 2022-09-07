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
    header :: OBJECT AS block_header_json,
    base_tables._inserted_timestamp :: TIMESTAMP AS _inserted_timestamp,
    state_tx_hash AS l1_state_root_tx_hash,
    state_batch_index AS l1_state_root_batch_index,
    l1_submission_tx_hash,
    l1_submission_batch_index AS l1_submission_batch_index
FROM
    base_tables
    LEFT JOIN {{ ref('bronze__state_hashes') }}
    ON block_id BETWEEN state_min_block
    AND state_max_block
    LEFT JOIN {{ ref('bronze__submission_hashes') }}
    ON block_id BETWEEN sub_min_block
    AND sub_max_block qualify(ROW_NUMBER() over(PARTITION BY block_number
ORDER BY
    base_tables._inserted_timestamp DESC)) = 1
