{{ config(
    materialized = 'view',
    tags = ['gha_workflows']
) }}

SELECT
    id,
    NAME,
    node_id,
    check_suite_id,
    check_suite_node_id,
    head_branch,
    head_sha,
    run_number,
    event,
    display_title,
    status,
    conclusion,
    workflow_id,
    url,
    html_url,
    pull_requests,
    created_at,
    updated_at,
    actor,
    run_attempt,
    run_started_at,
    triggering_actor,
    jobs_url,
    logs_url,
    check_suite_url,
    artifacts_url,
    cancel_url,
    rerun_url,
    workflow_url,
    head_commit,
    repository,
    head_repository,
    TIMESTAMPDIFF(seconds, run_started_at, SYSDATE()) / 60 AS delta_minutes
FROM
    TABLE(
        github_actions.tf_runs(
            'FlipsideCrypto',
            'optimism-models',{ 'status' :'in_progress' }
        )
    )
