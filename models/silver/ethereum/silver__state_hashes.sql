{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['state_block_timestamp::DATE'],
    tags = ['ethereum','non_realtime']
) }}

WITH base AS (

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
        {{ ref('bronze__state_hashes') }}

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
        TABLE(
            GENERATOR(
                rowcount => (
                    SELECT
                        MAX(block_number) AS max_block
                    FROM
                        {{ ref ('silver__blocks') }}
                )
            )
        )
)
SELECT
    block_number,
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
        ['block_number']
    ) }} AS state_hashes_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    blocks
    INNER JOIN base
    ON block_number BETWEEN state_min_block
    AND state_max_block QUALIFY(ROW_NUMBER() over(PARTITION BY block_number
    ORDER BY
    state_block_timestamp DESC)) = 1
