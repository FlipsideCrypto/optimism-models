{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "l1_submission_block_number",
    tags = ['bronze','ethereum']
) }}

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
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['l1_submission_block_number']
    ) }} AS submission_hashes_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ source(
        'ethereum_silver',
        'optimism_submission_hashes'
    ) }}

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