{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "state_block_number",
    tags = ['ethereum','non_realtime']
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
    _inserted_timestamp
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
