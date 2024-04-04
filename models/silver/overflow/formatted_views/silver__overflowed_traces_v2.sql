{{ config (
    materialized = 'view',
    tags = ['overflowed_traces']
) }}

WITH bronze_overflowed_traces AS (

    SELECT
        block_number :: INT AS block_number,
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
                'gasUsed',
                'gas',
                'type',
                'to',
                'from',
                'value',
                'input',
                'error',
                'output',
                'revertReason'
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
        ) AS str_array
    FROM
        {{ ref("bronze__overflowed_traces") }}
    GROUP BY
        block_number,
        tx_position,
        trace_address,
        _inserted_timestamp
),
sub_traces AS (
    SELECT
        block_number,
        tx_position,
        parent_trace_address,
        COUNT(*) AS sub_traces
    FROM
        bronze_overflowed_traces
    GROUP BY
        block_number,
        tx_position,
        parent_trace_address
),
num_array AS (
    SELECT
        block_number,
        tx_position,
        trace_address,
        ARRAY_AGG(flat_value) AS num_array
    FROM
        (
            SELECT
                block_number,
                tx_position,
                trace_address,
                IFF(
                    VALUE :: STRING = 'ORIGIN',
                    -1,
                    VALUE :: INT
                ) AS flat_value
            FROM
                bronze_overflowed_traces,
                LATERAL FLATTEN (
                    input => str_array
                )
        )
    GROUP BY
        block_number,
        tx_position,
        trace_address
),
cleaned_traces AS (
    SELECT
        b.block_number,
        b.tx_position,
        b.trace_address,
        IFNULL(
            sub_traces,
            0
        ) AS sub_traces,
        num_array,
        ROW_NUMBER() over (
            PARTITION BY b.block_number,
            b.tx_position
            ORDER BY
                num_array ASC
        ) - 1 AS trace_index,
        trace_json,
        b._inserted_timestamp
    FROM
        bronze_overflowed_traces b
        LEFT JOIN sub_traces s
        ON b.block_number = s.block_number
        AND b.tx_position = s.tx_position
        AND b.trace_address = s.parent_trace_address
        JOIN num_array n
        ON b.block_number = n.block_number
        AND b.tx_position = n.tx_position
        AND b.trace_address = n.trace_address
)
SELECT
    tx_position,
    trace_index,
    block_number,
    trace_address,
    trace_json :error :: STRING AS error_reason,
    trace_json :from :: STRING AS from_address,
    trace_json :to :: STRING AS to_address,
    IFNULL(
        utils.udf_hex_to_int(
            trace_json :value :: STRING
        ),
        '0'
    ) AS eth_value_precise_raw,
    utils.udf_decimal_adjust(
        eth_value_precise_raw,
        18
    ) AS eth_value_precise,
    eth_value_precise :: FLOAT AS eth_value,
    utils.udf_hex_to_int(
        trace_json :gas :: STRING
    ) :: INT AS gas,
    utils.udf_hex_to_int(
        trace_json :gasUsed :: STRING
    ) :: INT AS gas_used,
    trace_json :input :: STRING AS input,
    trace_json :output :: STRING AS output,
    trace_json :type :: STRING AS TYPE,
    concat_ws(
        '_',
        TYPE,
        trace_address
    ) AS identifier,
    concat_ws(
        '-',
        block_number,
        tx_position,
        identifier
    ) AS _call_id,
    _inserted_timestamp,
    trace_json AS DATA,
    sub_traces
FROM
    cleaned_traces
