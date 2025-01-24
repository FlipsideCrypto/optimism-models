-- depends_on: {{ ref('bronze__overflowed_traces') }}
{% set warehouse = 'DBT_SNOWPARK' if var('OVERFLOWED_TRACES') else target.warehouse %}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','tx_position'],
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    tags = ['overflowed_traces'],
    full_refresh = false,
    snowflake_warehouse = warehouse
) }}

{% if is_incremental() %}
WITH bronze_overflowed_traces AS (

    SELECT
        block_number :: INT AS block_number,
        ROUND(
            block_number,
            -3
        ) AS partition_key,
        index_vals [1] :: INT AS tx_position,
        IFF(
            path IN (
                'result',
                'result.value',
                'result.type',
                'result.to',
                'result.input',
                'result.gasUsed',
                'result.gas',
                'result.from',
                'result.output',
                'result.error',
                'result.revertReason',
                'result.time',
                'gasUsed',
                'gas',
                'type',
                'to',
                'from',
                'value',
                'input',
                'error',
                'output',
                'revertReason',
                'time'
            ),
            'ORIGIN',
            REGEXP_REPLACE(REGEXP_REPLACE(path, '[^0-9]+', '_'), '^_|_$', '')
        ) AS trace_address,
        SYSDATE() :: timestamp_ltz AS _inserted_timestamp,
        OBJECT_AGG(
            key,
            value_
        ) AS trace_json,
        CASE
            WHEN trace_address = 'ORIGIN' THEN NULL
            WHEN POSITION(
                '_' IN trace_address
            ) = 0 THEN 'ORIGIN'
            ELSE REGEXP_REPLACE(
                trace_address,
                '_[0-9]+$',
                '',
                1,
                1
            )
        END AS parent_trace_address,
        SPLIT(
            trace_address,
            '_'
        ) AS trace_address_array
    FROM
        {{ ref("bronze__overflowed_traces") }}
    GROUP BY
        block_number,
        tx_position,
        trace_address,
        _inserted_timestamp
)
SELECT
    block_number,
    tx_position,
    trace_address,
    parent_trace_address,
    trace_address_array,
    trace_json,
    partition_key,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'tx_position', 'trace_address']
    ) }} AS traces_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    bronze_overflowed_traces qualify(ROW_NUMBER() over(PARTITION BY traces_id
ORDER BY
    _inserted_timestamp DESC)) = 1
{% else %}
SELECT
    NULL :: INT AS block_number,
    NULL :: INT tx_position,
    NULL :: text AS trace_address,
    NULL :: text AS parent_trace_address,
    NULL :: ARRAY AS trace_address_array,
    NULL :: OBJECT AS trace_json,
    NULL :: INT AS partition_key,
    NULL :: timestamp_ltz AS _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'tx_position', 'trace_address']
    ) }} AS traces_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
{% endif %}
