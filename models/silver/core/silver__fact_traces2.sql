{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    incremental_predicates = [fsc_evm.standard_predicate()],
    cluster_by = "block_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['core','non_realtime'],
    full_refresh = false
) }}
{{ gold_traces_v1(
    full_reload_start_block = 30000000,
    full_reload_blocks = 10000000,
    uses_overflow_steps = true
) }}
