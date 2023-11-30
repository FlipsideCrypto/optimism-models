{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime']
) }}

SELECT
    r.block_number,
    l.block_timestamp,
    r.tx_hash,
    l.tx_status AS status,
    CASE
        WHEN topics [0] :: STRING = '0x3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f' THEN 'DelegateChanged'
        WHEN topics [0] :: STRING = '0xdec2bacdd2f05b59de34da9b523dff8be42e5e38e818c82fdb0bae774387a724' THEN 'DelegateVotesChanged'
    END AS event_name,
    from_address AS delegator,
    CASE
        WHEN CONCAT(
            '0x',
            SUBSTR(
                logs [0] :topics [2] :: STRING,
                27,
                40
            )
        ) = '0x0000000000000000000000000000000000000000'
        AND CONCAT(
            '0x',
            SUBSTR(
                logs [0] :topics [3] :: STRING,
                27,
                40
            )
        ) <> delegator THEN 'First Time Delegator'
        WHEN CONCAT(
            '0x',
            SUBSTR(
                logs [0] :topics [2] :: STRING,
                27,
                40
            )
        ) = '0x0000000000000000000000000000000000000000'
        AND delegator = CONCAT(
            '0x',
            SUBSTR(
                logs [0] :topics [3] :: STRING,
                27,
                40
            )
        ) THEN 'First Time Delegator - Self Delegation'
        WHEN delegator = CONCAT(
            '0x',
            SUBSTR(
                logs [0] :topics [3] :: STRING,
                27,
                40
            )
        ) THEN 'Self-Delegation'
        ELSE 'Re-Delegation'
    END AS delegation_type,
    CASE
        WHEN delegation_type = 'Re-Delegation'
        AND event_name = 'DelegateVotesChanged' THEN CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 40))
        ELSE CONCAT(
            '0x',
            SUBSTR(
                logs [0] :topics [3] :: STRING,
                27,
                40
            )
        )
    END AS to_delegate,
    CASE
        WHEN delegation_type = 'Re-Delegation' THEN CONCAT(
            '0x',
            SUBSTR(
                logs [0] :topics [2] :: STRING,
                27,
                40
            )
        )
        WHEN delegation_type = 'First Time Delegator'
        AND event_name = 'DelegateChanged' THEN CONCAT(
            '0x',
            SUBSTR(
                logs [0] :topics [2] :: STRING,
                27,
                40
            )
        )
        ELSE NULL
    END AS from_delegate,
    COALESCE(
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') [0] :: STRING)
            ),
            0
        ) AS raw_previous_balance,
        COALESCE(
            TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') [1] :: STRING)
                ),
                0
            ) AS raw_new_balance,
            COALESCE(raw_new_balance / pow(10, 18), 0) AS new_balance,
            COALESCE(raw_previous_balance / pow(10, 18), 0) AS previous_balance,
            r._inserted_timestamp,
            l._log_id,
            {{ dbt_utils.generate_surrogate_key(
                ['l._log_id']
            ) }} AS delegations_id,
            SYSDATE() AS inserted_timestamp,
            SYSDATE() AS modified_timestamp,
            '{{ invocation_id }}' AS _invocation_id
            FROM
                {{ ref('silver__receipts') }}
                r
                LEFT OUTER JOIN {{ ref('silver__logs') }}
                l
                ON r.tx_hash = l.tx_hash
            WHERE
                origin_function_signature = '0x5c19a95c'
                AND to_address = '0x4200000000000000000000000000000000000042'
                AND topics [0] :: STRING IN (
                    '0x3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f',
                    '0xdec2bacdd2f05b59de34da9b523dff8be42e5e38e818c82fdb0bae774387a724'
                )
                AND to_delegate IS NOT NULL

{% if is_incremental() %}
AND r._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
AND l._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
