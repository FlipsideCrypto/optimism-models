{{ config(
    materialized = 'incremental',
    unique_key = "_log_id",
    cluster_by = ['block_timestamp::DATE']
) }}

WITH delegate_votes_changed AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        tx_status AS status,
        origin_from_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS delegate_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        COALESCE(
            TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    segmented_data [0] :: STRING
                )
            ),
            0
        ) AS raw_previous_balance,
        COALESCE(
            TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    segmented_data [1] :: STRING
                )
            ),
            0
        ) AS raw_new_balance,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xdec2bacdd2f05b59de34da9b523dff8be42e5e38e818c82fdb0bae774387a724' --DelegateVotesChanged
        AND contract_address = '0x4200000000000000000000000000000000000042'
        AND origin_function_signature = '0x5c19a95c'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE
    FROM
        {{ this }}
)
{% endif %}
),
delegate_changed AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        tx_status AS status,
        origin_from_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS delegator_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS from_delegate_address,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS to_delegate_address,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f' --DelegateChanged
        AND contract_address = '0x4200000000000000000000000000000000000042'
        AND origin_function_signature = '0x5c19a95c'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                delegate_votes_changed
        )
)
SELECT
    v.block_number,
    v.block_timestamp,
    v.tx_hash,
    v.status,
    delegator_address AS delegator,
    delegate_address AS delegate,
    from_delegate_address AS from_delegate,
    to_delegate_address AS to_delegate,
    CASE
        WHEN from_delegate_address = '0x0000000000000000000000000000000000000000'
        AND to_delegate_address <> delegator THEN 'First Time Delegator'
        WHEN from_delegate_address = '0x0000000000000000000000000000000000000000'
        AND delegator = to_delegate_address THEN 'First Time Delegator - Self Delegation'
        WHEN delegator = to_delegate_address
        AND from_delegate_address <> '0x0000000000000000000000000000000000000000' THEN 'Self-Delegation'
        ELSE 'Re-Delegation'
    END AS delegation_type,
    COALESCE(
        raw_new_balance,
        0
    ) AS raw_new_balance,
    COALESCE(
        raw_previous_balance,
        0
    ) AS raw_previous_balance,
    COALESCE(raw_new_balance / pow(10, 18), 0) AS new_balance,
    COALESCE(raw_previous_balance / pow(10, 18), 0) AS previous_balance,
    v._log_id,
    v._inserted_timestamp
FROM
    delegate_votes_changed v
    LEFT JOIN delegate_changed d
    ON d.tx_hash = v.tx_hash
