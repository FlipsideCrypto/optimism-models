{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    incremental_predicates = [fsc_evm.standard_predicate()],
    cluster_by = "block_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['traces_reload'],
    full_refresh = false
) }}
{{ fsc_evm.gold_traces_v1(
    full_reload_start_block = 30000000,
    full_reload_blocks = 10000000,
    full_reload_mode = true,
    uses_overflow_steps = true
) }}
