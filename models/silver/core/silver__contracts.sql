{{ config(
    materialized = 'incremental',
    unique_key = 'contract_address',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['non_realtime']
) }}

WITH base_metadata AS (

    SELECT
        contract_address,
        block_number,
        function_sig AS function_signature,
        read_result AS read_output,
        _inserted_timestamp
    FROM
        {{ ref('bronze_api__token_reads') }}
    WHERE
        read_result IS NOT NULL
        AND read_result <> '0x'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),
token_names AS (
    SELECT
        contract_address,
        block_number,
        function_signature,
        read_output,
        utils.udf_hex_to_string(SUBSTR(read_output,(64 * 2 + 3), len(read_output))) AS token_name
    FROM
        base_metadata
    WHERE
        function_signature = '0x06fdde03'
        AND token_name IS NOT NULL),
        token_symbols AS (
            SELECT
                contract_address,
                block_number,
                function_signature,
                read_output,
                utils.udf_hex_to_string(SUBSTR(read_output,(64 * 2 + 3), len(read_output))) AS token_symbol
            FROM
                base_metadata
            WHERE
                function_signature = '0x95d89b41'
                AND token_symbol IS NOT NULL),
                token_decimals AS (
                    SELECT
                        contract_address,
                        utils.udf_hex_to_int(
                            read_output :: STRING
                        ) AS token_decimals,
                        LENGTH(token_decimals) AS dec_length
                    FROM
                        base_metadata
                    WHERE
                        function_signature = '0x313ce567'
                        AND read_output IS NOT NULL
                        AND read_output <> '0x'
                ),
                contracts AS (
                    SELECT
                        contract_address,
                        MAX(_inserted_timestamp) AS _inserted_timestamp
                    FROM
                        base_metadata
                    GROUP BY
                        1
                )
            SELECT
                c1.contract_address AS contract_address,
                token_name,
                token_decimals :: INTEGER AS token_decimals,
                token_symbol,
                _inserted_timestamp,
                {{ dbt_utils.generate_surrogate_key(
                    ['c1.contract_address']
                ) }} AS contracts_id,
                SYSDATE() AS inserted_timestamp,
                SYSDATE() AS modified_timestamp,
                '{{ invocation_id }}' AS _invocation_id
            FROM
                contracts c1
                LEFT JOIN token_names
                ON c1.contract_address = token_names.contract_address
                LEFT JOIN token_symbols
                ON c1.contract_address = token_symbols.contract_address
                LEFT JOIN token_decimals
                ON c1.contract_address = token_decimals.contract_address
                AND dec_length < 3 qualify(ROW_NUMBER() over(PARTITION BY c1.contract_address
            ORDER BY
                _inserted_timestamp DESC)) = 1
