{{ config(
    materialized = 'incremental',
    unique_key = 'test_timestamp',
    tags = ['observability']
) }}

WITH base AS (

    SELECT
        blocks_impacted_count
    FROM
        {{ ref('silver_observability__traces_completeness') }}
    WHERE
        test_timestamp > DATEADD('day', -5, CURRENT_TIMESTAMP())
    ORDER BY
        test_timestamp DESC
    LIMIT
        1), run_model AS (
            SELECT
                blocks_impacted_count,
                github_actions.workflow_dispatches(
                    'FlipsideCrypto',
                    'optimism-models',
                    'dbt_run_overflowed_traces.yml',
                    NULL
                ) AS run_overflow_models,
                github_actions.workflow_dispatches(
                    'FlipsideCrypto',
                    'optimism-models',
                    'dbt_run_overflowed_traces2.yml',
                    NULL
                ) AS run_overflow_models2
            FROM
                base
            WHERE
                blocks_impacted_count > 0
        )
    SELECT
        dummy,
        COALESCE(
            blocks_impacted_count,
            0
        ) AS blocks_impacted_count,
        COALESCE(
            run_overflow_models,
            OBJECT_CONSTRUCT(
                'status',
                'skipped'
            )
        ) AS run_overflow_models,
        COALESCE(
            run_overflow_models2,
            OBJECT_CONSTRUCT(
                'status',
                'skipped'
            )
        ) AS run_overflow_models2,
        SYSDATE() AS test_timestamp
    FROM
        (
            SELECT
                1 AS dummy
        )
        LEFT JOIN run_model
        ON 1 = 1
