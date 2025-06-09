create or replace view streamline.decoded_logs_history_2021_11 as (
          WITH target_blocks AS (
              SELECT 
                  block_number
              FROM OPTIMISM_DEV.core.fact_blocks
              WHERE date_trunc('month', block_timestamp) = '2021-11-01'::timestamp
          ),
          new_abis AS (
              SELECT 
                abi,
                parent_contract_address,
                event_signature,
                start_block,
                end_block
              FROM OPTIMISM_DEV.silver.complete_event_abis 
              
          ),
          existing_logs_to_exclude AS (
              SELECT _log_id
              FROM OPTIMISM_DEV.streamline.complete_decode_logs l
              INNER JOIN target_blocks b using (block_number)
          ),
          candidate_logs AS (
              SELECT 
                  l.block_number,
                  l.tx_hash,
                  l.event_index,
                  l.contract_address,
                  l.topics,
                  l.data,
                  concat(l.tx_hash::string, '-', l.event_index::string) as _log_id
              FROM target_blocks b
              INNER JOIN OPTIMISM_DEV.core.fact_event_logs l using (block_number)
              WHERE l.tx_status = 'SUCCESS' and date_trunc('month', l.block_timestamp) = '2021-11-01'::timestamp
          )
          SELECT
            l.block_number,
            l._log_id,
            A.abi,
            OBJECT_CONSTRUCT(
              'topics', l.topics,
              'data', l.data,
              'address', l.contract_address
            ) AS data
          FROM candidate_logs l
          INNER JOIN new_abis A
            ON A.parent_contract_address = l.contract_address
            AND A.event_signature = l.topics[0]::STRING
            AND l.block_number BETWEEN A.start_block AND A.end_block
          WHERE NOT EXISTS (
              SELECT 1 
              FROM existing_logs_to_exclude e 
              WHERE e._log_id = l._log_id
          )
          LIMIT 7500000
        )
