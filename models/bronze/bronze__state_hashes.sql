{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "state_block_number",
    tags = ['bronze','ethereum']
) }}

SELECT
    state_tx_hash,
    state_block_number,
    state_block_timestamp,
    state_batch_index,
    state_batch_root,
    bedrock_state_batch_index,
    bedrock_state_batch_root,
    state_batch_size,
    state_prev_total_elements,
    state_min_block,
    state_max_block,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['state_block_number']
    ) }} AS state_hashes_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ source(
        'ethereum_silver',
        'optimism_state_hashes'
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