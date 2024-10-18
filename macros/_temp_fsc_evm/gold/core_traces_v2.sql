
{% macro gold_traces_v2(
        full_reload_start_block,
        full_reload_blocks,
        full_reload_mode = false,
        uses_overflow_steps = false,
        TRACES_ARB_MODE = false,
        TRACES_KAIA_MODE = false,
        schema_name = 'silver',
        uses_tx_status = false,
        TRACES_SEI_MODE = false
    ) %}
    WITH silver_traces AS (
        SELECT
            block_number,
            {% if TRACES_SEI_MODE %}
                tx_hash,
            {% else %}
                tx_position,
            {% endif %}
            trace_address,
            parent_trace_address,
            trace_address_array,
            trace_json,
            traces_id,
            'regular' AS source
        FROM
            {{ ref(
                schema_name ~ '__traces'
            ) }}
        WHERE
            1 = 1

{% if is_incremental() and not full_reload_mode %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
) {% elif is_incremental() and full_reload_mode %}
AND block_number BETWEEN (
    SELECT
        MAX(
            block_number
        )
    FROM
        {{ this }}
)
AND (
    SELECT
        MAX(
            block_number
        ) + {{ full_reload_blocks }}
    FROM
        {{ this }}
)
{% else %}
    AND block_number <= {{ full_reload_start_block }}
{% endif %}

{% if uses_overflow_steps %}
UNION ALL
SELECT
    block_number,
    {% if TRACES_SEI_MODE %}
        tx_hash,
    {% else %}
        tx_position,
    {% endif %}
    trace_address,
    parent_trace_address,
    trace_address_array,
    trace_json,
    traces_id,
    'overflow' AS source
FROM
    {{ ref(
        schema_name ~ '__overflowed_traces'
    ) }}
WHERE
    1 = 1

{% if is_incremental() and not full_reload_mode %}
AND modified_timestamp > (
    SELECT
        DATEADD('hour', -2, MAX(modified_timestamp))
    FROM
        {{ this }}) {% elif is_incremental() and full_reload_mode %}
        AND block_number BETWEEN (
            SELECT
                MAX(
                    block_number
                )
            FROM
                {{ this }}
        )
        AND (
            SELECT
                MAX(
                    block_number
                ) + {{ full_reload_blocks }}
            FROM
                {{ this }}
        )
    {% else %}
        AND block_number <= {{ full_reload_start_block }}
    {% endif %}
    {% endif %}

    {% if TRACES_ARB_MODE %}
    UNION ALL
    SELECT
        block_number,
        tx_position,
        trace_address,
        parent_trace_address,
        IFF(
            trace_address = 'ORIGIN',
            ARRAY_CONSTRUCT('ORIGIN'),
            trace_address_array
        ) AS trace_address_array,
        trace_json,
        traces_id,
        'arb_traces' AS source
    FROM
        {{ ref('silver__arb_traces') }}
    WHERE
        1 = 1

{% if is_incremental() and not full_reload_mode %}
AND modified_timestamp > (
    SELECT
        DATEADD('hour', -2, MAX(modified_timestamp))
    FROM
        {{ this }}) {% elif is_incremental() and full_reload_mode %}
        AND block_number BETWEEN (
            SELECT
                MAX(
                    block_number
                )
            FROM
                {{ this }}
        )
        AND (
            SELECT
                MAX(
                    block_number
                ) + {{ full_reload_blocks }}
            FROM
                {{ this }}
        )
    {% else %}
        AND block_number <= {{ full_reload_start_block }}
    {% endif %}
    {% endif %}
),
sub_traces AS (
    SELECT
        block_number,
        {% if TRACES_SEI_MODE %}
            tx_hash,
        {% else %}
            tx_position,
        {% endif %}
        parent_trace_address,
        COUNT(*) AS sub_traces
    FROM
        silver_traces
    GROUP BY
        block_number,
        {% if TRACES_SEI_MODE %}
            tx_hash,
        {% else %}
            tx_position,
        {% endif %}
        parent_trace_address
),
trace_index_array AS (
    SELECT
        block_number,
        {% if TRACES_SEI_MODE %}
            tx_hash,
        {% else %}
            tx_position,
        {% endif %}
        trace_address,
        ARRAY_AGG(flat_value) AS number_array
    FROM
        (
            SELECT
                block_number,
                {% if TRACES_SEI_MODE %}
                    tx_hash,
                {% else %}
                    tx_position,
                {% endif %}
                trace_address,
                IFF(
                    VALUE :: STRING = 'ORIGIN',
                    -1,
                    VALUE :: INT
                ) AS flat_value
            FROM
                silver_traces,
                LATERAL FLATTEN (
                    input => trace_address_array
                )
        )
    GROUP BY
        block_number,
        {% if TRACES_SEI_MODE %}
            tx_hash,
        {% else %}
            tx_position,
        {% endif %}
        trace_address
),
trace_index_sub_traces AS (
    SELECT
        b.block_number,
        {% if TRACES_SEI_MODE %}
            b.tx_hash,
        {% else %}
            b.tx_position,
        {% endif %}
        b.trace_address,
        IFNULL(
            sub_traces,
            0
        ) AS sub_traces,
        number_array,
        ROW_NUMBER() over (
            PARTITION BY b.block_number,
            {% if TRACES_SEI_MODE %}
                b.tx_hash,
            {% else %}
                b.tx_position,
            {% endif %}
            ORDER BY
                number_array ASC
        ) - 1 AS trace_index,
        b.trace_json,
        b.traces_id,
        b.source
    FROM
        silver_traces b
        LEFT JOIN sub_traces s
        ON b.block_number = s.block_number
        AND {% if TRACES_SEI_MODE %}
                b.tx_hash = s.tx_hash
            {% else %}
                b.tx_position = s.tx_position
            {% endif %}
        AND b.trace_address = s.parent_trace_address
        JOIN trace_index_array n
        ON b.block_number = n.block_number
        AND {% if TRACES_SEI_MODE %}
                b.tx_hash = n.tx_hash
            {% else %}
                b.tx_position = n.tx_position
            {% endif %}
        AND b.trace_address = n.trace_address
),
errored_traces AS (
    SELECT
        block_number,
        {% if TRACES_SEI_MODE %}
            tx_hash,
        {% else %}
            tx_position,
        {% endif %}
        trace_address,
        trace_json
    FROM
        trace_index_sub_traces
    WHERE
        trace_json :error :: STRING IS NOT NULL
),
error_logic AS (
    SELECT
        b0.block_number,
        {% if TRACES_SEI_MODE %}
            b0.tx_hash,
        {% else %}
            b0.tx_position,
        {% endif %}
        b0.trace_address,
        b0.trace_json :error :: STRING AS error,
        b1.trace_json :error :: STRING AS any_error,
        b2.trace_json :error :: STRING AS origin_error
    FROM
        trace_index_sub_traces b0
        LEFT JOIN errored_traces b1
        ON b0.block_number = b1.block_number
        {% if TRACES_SEI_MODE %}
            AND b0.tx_hash = b1.tx_hash
        {% else %}
            AND b0.tx_position = b1.tx_position
        {% endif %}
        AND b0.trace_address LIKE CONCAT(
            b1.trace_address,
            '_%'
        )
        LEFT JOIN errored_traces b2
        ON b0.block_number = b2.block_number
        {% if TRACES_SEI_MODE %}
            AND b0.tx_hash = b2.tx_hash
        {% else %}
            AND b0.tx_position = b2.tx_position
        {% endif %}
        AND b2.trace_address = 'ORIGIN'
),
aggregated_errors AS (
    SELECT
        block_number,
        {% if TRACES_SEI_MODE %}
            tx_hash,
        {% else %}
            tx_position,
        {% endif %}
        trace_address,
        error,
        IFF(MAX(any_error) IS NULL
        AND error IS NULL
        AND origin_error IS NULL, TRUE, FALSE) AS trace_succeeded
    FROM
        error_logic
    GROUP BY
        block_number,
        {% if TRACES_SEI_MODE %}
            tx_hash,
        {% else %}
            tx_position,
        {% endif %}
        trace_address,
        error,
        origin_error),
        json_traces AS {% if not TRACES_ARB_MODE %}
            (
                SELECT
                    block_number,
                    {% if TRACES_SEI_MODE %}
                        tx_hash,
                    {% else %}
                        tx_position,
                    {% endif %}
                    trace_address,
                    sub_traces,
                    number_array,
                    trace_index,
                    trace_succeeded,
                    trace_json :error :: STRING AS error_reason,
                    {% if TRACES_KAIA_MODE %}
                        coalesce(
                            trace_json :revertReason :: STRING,
                            trace_json :reverted :message :: STRING
                        ) AS revert_reason,
                    {% else %}
                        trace_json :revertReason :: STRING AS revert_reason,
                    {% endif %}
                    trace_json :from :: STRING AS from_address,
                    trace_json :to :: STRING AS to_address,
                    IFNULL(
                        trace_json :value :: STRING,
                        '0x0'
                    ) AS value_hex,
                    IFNULL(
                        utils.udf_hex_to_int(
                            trace_json :value :: STRING
                        ),
                        '0'
                    ) AS value_precise_raw,
                    utils.udf_decimal_adjust(
                        value_precise_raw,
                        18
                    ) AS value_precise,
                    value_precise :: FLOAT AS VALUE,
                    utils.udf_hex_to_int(
                        trace_json :gas :: STRING
                    ) :: INT AS gas,
                    utils.udf_hex_to_int(
                        trace_json :gasUsed :: STRING
                    ) :: INT AS gas_used,
                    trace_json :input :: STRING AS input,
                    trace_json :output :: STRING AS output,
                    trace_json :type :: STRING AS TYPE,
                    traces_id
                FROM
                    trace_index_sub_traces
                    JOIN aggregated_errors USING (
                        block_number,
                        {% if TRACES_SEI_MODE %}
                            tx_hash,
                        {% else %}
                            tx_position,
                        {% endif %}
                        trace_address
                    )
                {% else %}
                    (
                        SELECT
                            block_number,
                            tx_position,
                            trace_address,
                            sub_traces,
                            number_array,
                            trace_index,
                            trace_succeeded,
                            trace_json :error :: STRING AS error_reason,
                            trace_json :revertReason :: STRING AS revert_reason,
                            trace_json :from :: STRING AS from_address,
                            trace_json :to :: STRING AS to_address,
                            IFNULL(
                                trace_json :value :: STRING,
                                '0x0'
                            ) AS value_hex,
                            IFNULL(
                                utils.udf_hex_to_int(
                                    trace_json :value :: STRING
                                ),
                                '0'
                            ) AS value_precise_raw,
                            utils.udf_decimal_adjust(
                                value_precise_raw,
                                18
                            ) AS value_precise,
                            value_precise :: FLOAT AS VALUE,
                            utils.udf_hex_to_int(
                                trace_json :gas :: STRING
                            ) :: INT AS gas,
                            utils.udf_hex_to_int(
                                trace_json :gasUsed :: STRING
                            ) :: INT AS gas_used,
                            trace_json :input :: STRING AS input,
                            trace_json :output :: STRING AS output,
                            trace_json :type :: STRING AS TYPE,
                            traces_id,
                            trace_json :afterEVMTransfers AS after_evm_transfers,
                            trace_json :beforeEVMTransfers AS before_evm_transfers
                        FROM
                            trace_index_sub_traces t0
                            JOIN aggregated_errors USING (
                                block_number,
                                tx_position,
                                trace_address
                            )
                        WHERE
                            t0.source <> 'arb_traces'
                        UNION ALL
                        SELECT
                            block_number,
                            tx_position,
                            trace_address,
                            sub_traces,
                            number_array,
                            trace_index,
                            trace_succeeded,
                            trace_json :error :: STRING AS error_reason,
                            NULL AS revert_reason,
                            trace_json :action :from :: STRING AS from_address,
                            COALESCE(
                                trace_json :action :to :: STRING,
                                trace_json :result :address :: STRING
                            ) AS to_address,
                            IFNULL(
                                trace_json :action :value :: STRING,
                                '0x0'
                            ) AS value_hex,
                            IFNULL(
                                utils.udf_hex_to_int(
                                    trace_json :action :value :: STRING
                                ),
                                '0'
                            ) AS value_precise_raw,
                            utils.udf_decimal_adjust(
                                value_precise_raw,
                                18
                            ) AS value_precise,
                            value_precise :: FLOAT AS VALUE,
                            utils.udf_hex_to_int(
                                trace_json :action :gas :: STRING
                            ) :: INT AS gas,
                            IFNULL(
                                utils.udf_hex_to_int(
                                    trace_json :result :gasUsed :: STRING
                                ),
                                0
                            ) :: INT AS gas_used,
                            COALESCE(
                                trace_json :action :input :: STRING,
                                trace_json :action :init :: STRING
                            ) AS input,
                            COALESCE(
                                trace_json :result :output :: STRING,
                                trace_json :result :code :: STRING
                            ) AS output,
                            UPPER(
                                COALESCE(
                                    trace_json :action :callType :: STRING,
                                    trace_json :type :: STRING
                                )
                            ) AS TYPE,
                            traces_id,
                            NULL AS after_evm_transfers,
                            NULL AS before_evm_transfers
                        FROM
                            trace_index_sub_traces t0
                            JOIN aggregated_errors USING (
                                block_number,
                                tx_position,
                                trace_address
                            )
                        WHERE
                            t0.source = 'arb_traces'
                        {% endif %}
                    ),
                    incremental_traces AS (
                        SELECT
                            f.block_number,
                            {% if TRACES_SEI_MODE %}
                                f.tx_hash,
                            {% else %}
                                t.tx_hash,
                            {% endif %}
                            t.block_timestamp,
                            t.origin_function_signature,
                            t.from_address AS origin_from_address,
                            t.to_address AS origin_to_address,
                            {% if TRACES_SEI_MODE %}
                                t.position AS tx_position,
                            {% else %}
                            f.tx_position,
                            {% endif %}
                            f.trace_index,
                            f.from_address AS from_address,
                            f.to_address AS to_address,
                            f.value_hex,
                            f.value_precise_raw,
                            f.value_precise,
                            f.value,
                            f.gas,
                            f.gas_used,
                            f.input,
                            f.output,
                            f.type,
                            f.sub_traces,
                            f.error_reason,
                            f.revert_reason,
                            f.traces_id,
                            f.trace_succeeded,
                            f.trace_address,
                            {% if uses_tx_status %}
                            t.tx_status AS tx_succeeded
                            {% else %}
                            t.tx_succeeded
                            {% endif %}
                            {% if TRACES_ARB_MODE %},
                            f.before_evm_transfers,
                            f.after_evm_transfers
                        {% endif %}
                        FROM
                            json_traces f
                            LEFT OUTER JOIN {{ ref(
                                schema_name ~ '__transactions'
                            ) }}
                            t
                            ON {% if TRACES_SEI_MODE %}
                                f.tx_hash = t.tx_hash
                            {% else %}
                                f.tx_position = t.position
                            {% endif %}
                            AND f.block_number = t.block_number

{% if is_incremental() and not full_reload_mode %}
AND t.modified_timestamp >= (
    SELECT
        DATEADD('hour', -24, MAX(modified_timestamp))
    FROM
        {{ this }})
    {% endif %}
)

{% if is_incremental() %},
overflow_blocks AS (
    SELECT
        DISTINCT block_number
    FROM
        silver_traces
    WHERE
        source = 'overflow'
),
heal_missing_data AS (
    SELECT
        t.block_number,
        {% if TRACES_SEI_MODE %}
            t.tx_hash,
        {% else %}
            txs.tx_hash,
        {% endif %}
        txs.block_timestamp,
        txs.origin_function_signature,
        txs.from_address AS origin_from_address,
        txs.to_address AS origin_to_address,
        {% if TRACES_SEI_MODE %}
            txs.position AS tx_position,
        {% else %}
            t.tx_position,
        {% endif %}
        t.trace_index,
        t.from_address,
        t.to_address,
        t.value_hex,
        t.value_precise_raw,
        t.value_precise,
        t.value,
        t.gas,
        t.gas_used,
        t.input,
        t.output,
        t.type,
        t.sub_traces,
        t.error_reason,
        t.revert_reason,
        t.fact_traces_id AS traces_id,
        t.trace_succeeded,
        t.trace_address,
        {% if uses_tx_status %}
        txs.tx_status AS tx_succeeded
        {% else %}
        txs.tx_succeeded
        {% endif %}
        {% if TRACES_ARB_MODE %},
        t.before_evm_transfers,
        t.after_evm_transfers
    {% endif %}
    FROM
        {{ this }}
        t
        JOIN {{ ref(
            schema_name ~ '__transactions'
        ) }}
        txs
        {% if TRACES_SEI_MODE %}
            ON t.tx_hash = txs.tx_hash
        {% else %}
            ON t.tx_position = txs.position
        {% endif %}
        AND t.block_
        AND t.block_number = txs.block_number
    WHERE
        {% if TRACES_SEI_MODE %}
            t.tx_position IS NULL
        {% else %}
            t.tx_hash IS NULL
        {% endif %}
        OR t.block_timestamp IS NULL
        OR t.tx_succeeded IS NULL
)
{% endif %},
all_traces AS (
    SELECT
        block_number,
        tx_hash,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_position,
        trace_index,
        from_address,
        to_address,
        value_hex,
        value_precise_raw,
        value_precise,
        VALUE,
        gas,
        gas_used,
        input,
        output,
        TYPE,
        sub_traces,
        error_reason,
        revert_reason,
        trace_succeeded,
        trace_address,
        tx_succeeded

        {% if TRACES_ARB_MODE %},
        before_evm_transfers,
        after_evm_transfers
    {% endif %}
    FROM
        incremental_traces

{% if is_incremental() %}
UNION ALL
SELECT
    block_number,
    tx_hash,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_position,
    trace_index,
    from_address,
    to_address,
    value_hex,
    value_precise_raw,
    value_precise,
    VALUE,
    gas,
    gas_used,
    input,
    output,
    TYPE,
    sub_traces,
    error_reason,
    revert_reason,
    trace_succeeded,
    trace_address,
    tx_succeeded

    {% if TRACES_ARB_MODE %},
    before_evm_transfers,
    after_evm_transfers
{% endif %}
FROM
    heal_missing_data
UNION ALL
SELECT
    block_number,
    tx_hash,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_position,
    trace_index,
    from_address,
    to_address,
    value_hex,
    value_precise_raw,
    value_precise,
    VALUE,
    gas,
    gas_used,
    input,
    output,
    TYPE,
    sub_traces,
    error_reason,
    revert_reason,
    trace_succeeded,
    trace_address,
    tx_succeeded

    {% if TRACES_ARB_MODE %},
    before_evm_transfers,
    after_evm_transfers
{% endif %}
FROM
    {{ this }}
    JOIN overflow_blocks USING (block_number)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    tx_position,
    trace_index,
    from_address,
    to_address,
    input,
    output,
    TYPE,
    trace_address,
    sub_traces,
    VALUE,
    value_precise_raw,
    value_precise,
    value_hex,
    gas,
    gas_used,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    {% if TRACES_ARB_MODE %}
        before_evm_transfers,
        after_evm_transfers,
    {% endif %}

    trace_succeeded,
    error_reason,
    revert_reason,
    tx_succeeded,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'trace_index']
    ) }} AS fact_traces_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    all_traces qualify(ROW_NUMBER() over(PARTITION BY block_number,  {% if TRACES_SEI_MODE %}tx_hash, {% else %}tx_position, {% endif %} trace_index
ORDER BY
    modified_timestamp DESC, block_timestamp DESC nulls last)) = 1
{% endmacro %}
