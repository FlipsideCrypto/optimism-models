{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['l1_submission_block_timestamp::DATE'],
    tags = ['silver','ethereum']
) }}

WITH base AS (

    SELECT
        l1_submission_tx_hash,
        l1_submission_block_number,
        l1_submission_block_timestamp,
        l1_submission_batch_index,
        l1_submission_batch_root,
        l1_submission_size,
        l1_submission_prev_total_elements,
        sub_min_block,
        sub_max_block,
        _inserted_timestamp
    FROM
        {{ ref('bronze__submission_hashes') }}

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
blocks AS (
    SELECT
        SEQ4() AS block_number
    FROM
        TABLE(GENERATOR(rowcount => 106000000))
)
SELECT
    block_number,
    l1_submission_tx_hash,
    l1_submission_block_number,
    l1_submission_block_timestamp,
    l1_submission_batch_index,
    l1_submission_batch_root,
    l1_submission_size,
    l1_submission_prev_total_elements,
    sub_min_block,
    sub_max_block,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number']
    ) }} AS submission_hashes_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    blocks
    INNER JOIN base
    ON block_number BETWEEN sub_min_block
    AND sub_max_block