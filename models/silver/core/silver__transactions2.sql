-- depends_on: {{ ref('bronze__streamline_transactions') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = "block_timestamp::date, _inserted_timestamp::date",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['core'],
    full_refresh = false
) }}

WITH base AS (

    SELECT
        block_number,
        DATA,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_transactions') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
    AND IS_OBJECT(DATA)
{% else %}
    {{ ref('bronze__streamline_FR_transactions') }}
WHERE
    IS_OBJECT(DATA)
{% endif %}
),
base_tx AS (
    SELECT
        A.block_number AS block_number,
        A.data :blockHash :: STRING AS block_hash,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                A.data :blockNumber :: STRING
            )
        ) AS blockNumber,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                A.data :chainId :: STRING
            )
        ) AS chain_id,
        A.data :from :: STRING AS from_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                A.data :gas :: STRING
            )
        ) AS gas,
        COALESCE(
            TRY_TO_NUMBER(
                utils.udf_hex_to_int(
                    A.data :gasPrice :: STRING
                )
            ),
            0
        ) AS gas_price,
        A.data :hash :: STRING AS tx_hash,
        A.data :input :: STRING AS input_data,
        SUBSTR(
            input_data,
            1,
            10
        ) AS origin_function_signature,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                A.data :maxFeePerGas :: STRING
            )
        ) / pow(
            10,
            9
        ) AS max_fee_per_gas,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                A.data :maxPriorityFeePerGas :: STRING
            )
        ) / pow(
            10,
            9
        ) AS max_priority_fee_per_gas,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                A.data :nonce :: STRING
            )
        ) AS nonce,
        A.data :r :: STRING AS r,
        A.data :s :: STRING AS s,
        A.data :to :: STRING AS to_address1,
        CASE
            WHEN to_address1 = '' THEN NULL
            ELSE to_address1
        END AS to_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                A.data :transactionIndex :: STRING
            )
        ) AS POSITION,
        A.data :type :: STRING AS TYPE,
        A.data :v :: STRING AS v,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                A.data :value :: STRING
            )
        ) / pow(
            10,
            18
        ) :: FLOAT AS VALUE,
        A._INSERTED_TIMESTAMP
    FROM
        base A
),
new_records AS (
    SELECT
        t.block_number,
        t.block_hash,
        t.chain_id,
        t.from_address,
        t.gas,
        t.gas_price / pow(
            10,
            9
        ) AS gas_price,
        t.gas_price AS gas_price1,
        t.tx_hash,
        t.input_data,
        t.origin_function_signature,
        t.max_fee_per_gas,
        t.max_priority_fee_per_gas,
        t.nonce,
        t.r,
        t.s,
        t.to_address,
        t.position,
        t.type,
        t.v,
        t.value,
        block_timestamp,
        CASE
            WHEN block_timestamp IS NULL
            OR tx_status IS NULL THEN TRUE
            ELSE FALSE
        END AS is_pending,
        r.gas_used,
        r.l1_fee / pow(
            10,
            18
        ) AS l1_fee,
        r.l1_fee_scalar,
        r.l1_gas_used,
        r.l1_gas_price / pow(
            10,
            9
        ) AS l1_gas_price,
        r.l1_gas_price AS l1_gas_price1,
        COALESCE(
            ((r.gas_used * gas_price1) + (l1_gas_price1 * r.l1_gas_used * r.l1_fee_scalar)) / pow(
                10,
                18
            ),
            0
        ) AS tx_fee,
        tx_success,
        tx_status,
        cumulative_gas_used,
        effective_gas_price,
        r.type AS tx_type,
        t._inserted_timestamp
    FROM
        base_tx t
        LEFT OUTER JOIN {{ ref('silver__blocks2') }}
        b
        ON t.block_number = b.block_number
        LEFT OUTER JOIN {{ ref('silver__receipts') }}
        r
        ON t.block_number = r.block_number
        AND t.tx_hash = r.tx_hash

{% if is_incremental() %}
AND r._INSERTED_TIMESTAMP >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
)

{% if is_incremental() %},
missing_data AS (
    SELECT
        t.block_number,
        t.block_hash,
        t.chain_id,
        t.from_address,
        t.gas,
        t.gas_price,
        t.tx_hash,
        t.input_data,
        t.origin_function_signature,
        t.max_fee_per_gas,
        t.max_priority_fee_per_gas,
        t.nonce,
        t.r,
        t.s,
        t.to_address,
        t.position,
        t.type,
        t.v,
        t.value,
        b.block_timestamp,
        FALSE AS is_pending,
        r.gas_used,
        r.tx_success,
        r.tx_status,
        r.cumulative_gas_used,
        r.effective_gas_price,
        t.l1_fee,
        t.l1_fee_scalar,
        t.l1_gas_used,
        t.l1_gas_price,
        t.tx_fee,
        r.type AS tx_type,
        GREATEST(
            t._inserted_timestamp,
            b._inserted_timestamp,
            r._inserted_timestamp
        ) AS _inserted_timestamp
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('silver__blocks2') }}
        b
        ON t.block_number = b.block_number
        INNER JOIN {{ ref('silver__receipts') }}
        r
        ON t.tx_hash = r.tx_hash
        AND t.block_number = r.block_number
    WHERE
        t.is_pending
)
{% endif %},
exclusions AS (
    SELECT
        CONCAT(
            block_number,
            '-',
            tx_hash
        ) AS block_tx_id
    FROM
        {{ ref('silver_observability__excluded_receipt_blocks') }}
),
FINAL AS (
    SELECT
        block_number,
        block_hash,
        chain_id,
        from_address,
        gas,
        gas_price,
        tx_hash,
        input_data,
        origin_function_signature,
        max_fee_per_gas,
        max_priority_fee_per_gas,
        nonce,
        r,
        s,
        to_address,
        POSITION,
        TYPE,
        v,
        VALUE,
        block_timestamp,
        is_pending,
        gas_used,
        tx_success,
        tx_status,
        cumulative_gas_used,
        effective_gas_price,
        l1_fee,
        l1_fee_scalar,
        l1_gas_used,
        l1_gas_price,
        tx_fee,
        tx_type,
        _inserted_timestamp
    FROM
        new_records

{% if is_incremental() %}
UNION
SELECT
    block_number,
    block_hash,
    chain_id,
    from_address,
    gas,
    gas_price,
    tx_hash,
    input_data,
    origin_function_signature,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    nonce,
    r,
    s,
    to_address,
    POSITION,
    TYPE,
    v,
    VALUE,
    block_timestamp,
    is_pending,
    gas_used,
    tx_success,
    tx_status,
    cumulative_gas_used,
    effective_gas_price,
    l1_fee,
    l1_fee_scalar,
    l1_gas_used,
    l1_gas_price,
    tx_fee,
    tx_type,
    _inserted_timestamp
FROM
    missing_data
{% endif %}
)
SELECT
    block_number,
    block_hash,
    chain_id,
    from_address,
    gas,
    gas_price,
    tx_hash,
    input_data,
    origin_function_signature,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    nonce,
    r,
    s,
    to_address,
    POSITION,
    TYPE,
    v,
    VALUE,
    block_timestamp,
    CASE
        WHEN CONCAT(
            block_number,
            '-',
            tx_hash
        ) IN (
            SELECT
                block_tx_id
            FROM
                exclusions
        ) THEN FALSE
        ELSE is_pending
    END AS is_pending,
    gas_used,
    tx_success,
    tx_status,
    cumulative_gas_used,
    effective_gas_price,
    l1_fee,
    l1_fee_scalar,
    l1_gas_used,
    l1_gas_price,
    tx_fee,
    tx_type,
    _inserted_timestamp
FROM
    FINAL qualify(ROW_NUMBER() over (PARTITION BY block_number, POSITION
ORDER BY
    _inserted_timestamp DESC, is_pending ASC)) = 1
