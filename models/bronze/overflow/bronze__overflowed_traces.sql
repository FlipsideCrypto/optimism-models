{{ config (
    materialized = "view",
    tags = ['overflowed_traces']
) }}

{% for item in range(
        1,
        11
    ) %}

    SELECT
        o.file_name,
        f.block_number,
        f.index_vals,
        f.path,
        f.key,
        f.value_
    FROM
        (
            SELECT
                file_name,
                file_url,
                index_cols,
                [overflowed_block, overflowed_tx] AS index_vals
            FROM
                (
                    SELECT
                        block_number,
                        POSITION,
                        file_name,
                        file_url,
                        index_cols,
                        VALUE [0] AS overflowed_block,
                        VALUE [1] AS overflowed_tx,
                        block_number = overflowed_block
                        AND POSITION = overflowed_tx AS missing
                    FROM
                        (
                            SELECT
                                block_number,
                                POSITION,
                                file_name,
                                file_url,
                                index_cols,
                                utils.udf_detect_overflowed_responses(
                                    file_url,
                                    index_cols
                                ) AS index_vals
                            FROM
                                {{ ref("bronze__potential_overflowed_traces") }}
                            WHERE
                                row_no = {{ item }}
                        ),
                        LATERAL FLATTEN (
                            input => index_vals
                        )
                )
            WHERE
                missing = TRUE
        ) o,
        TABLE(
            utils.udtf_flatten_overflowed_responses(
                o.file_url,
                o.index_cols,
                [o.index_vals]
            )
        ) f
    WHERE
        NOT IS_OBJECT(
            f.value_
        )
        AND NOT IS_ARRAY(
            f.value_
        )
        AND NOT IS_NULL_VALUE(
            f.value_
        ) {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
