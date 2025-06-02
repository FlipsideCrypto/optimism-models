    {{ config(
        materialized = 'incremental',
        unique_key = "compound_market_address",
        tags = ['silver','defi','lending','curated','asset_details']
    ) }}

    WITH contracts_dim AS (
        SELECT
            address,
            name,
            symbol,
            decimals
        FROM
            {{ ref('core__dim_contracts') }}
    ),

    comp_v3_base AS (
        SELECT
            contract_address,
            block_number,
            live.udf_api(
                'POST',
                '{URL}',
                OBJECT_CONSTRUCT(
                    'Content-Type', 'application/json',
                    'fsc-quantum-state', 'livequery'
                ),
                utils.udf_json_rpc_call(
                    'eth_call',
                    [
                        {
                            'to': contract_address, 
                            'from': null, 
                            'data': RPAD('0xc55dae63', 64, '0')
                        }, 
                        utils.udf_int_to_hex(block_number)
                    ],
                    concat_ws('-', contract_address, '0xc55dae63', block_number)
                ),
                'Vault/prod/evm/quicknode/optimism/mainnet'
            ) AS api_response
        FROM
            {{ ref('core__fact_event_logs') }}
        WHERE
            topic_0 = '0xbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b'
            AND origin_from_address IN (
                LOWER('0x6103DB328d4864dc16BD2F0eE1B9A92e3F87f915'),
                LOWER('0x2501713A67a3dEdde090E42759088A7eF37D4EAb')
            )
            
        {% if is_incremental() %}
        AND modified_timestamp >= (
            SELECT MAX(modified_timestamp) - INTERVAL '12 hours' FROM {{ this }}
        )
        AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
        {% endif %}

        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY contract_address
            ORDER BY block_number ASC
        ) = 1
    ),

    comp_v3_data AS (
        SELECT
            l.contract_address AS ctoken_address,
            c1.symbol AS ctoken_symbol,
            c1.name AS ctoken_name,
            c1.decimals AS ctoken_decimals,
            LOWER(
                CONCAT(
                    '0x',
                    SUBSTR(
                        l.api_response:data:result :: STRING,
                        -40
                    )
                )
            ) AS underlying_address,
            c2.name AS underlying_name,
            c2.symbol AS underlying_symbol,
            c2.decimals AS underlying_decimals,
            l.block_number AS created_block,
            'Compound V3' AS compound_version
        FROM comp_v3_base l
        LEFT JOIN contracts_dim c1 ON l.contract_address = c1.address
        LEFT JOIN contracts_dim c2 ON LOWER(
            CONCAT(
                '0x',
                SUBSTR(
                    l.api_response:data:result :: STRING,
                    -40
                )
            )
        ) = c2.address
        WHERE c1.name IS NOT NULL
    )

    SELECT
        ctoken_address AS compound_market_address,
        ctoken_symbol AS compound_market_symbol,
        ctoken_name AS compound_market_name,
        ctoken_decimals AS compound_market_decimals,
        underlying_address AS underlying_asset_address,
        underlying_name AS underlying_asset_name,
        underlying_symbol AS underlying_asset_symbol,
        underlying_decimals AS underlying_asset_decimals,
        created_block AS created_block_number,
        compound_version,
        {{ dbt_utils.generate_surrogate_key(['compound_market_address']) }} AS comp_asset_details_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        comp_v3_data 
