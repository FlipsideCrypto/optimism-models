DBT_TARGET ?= dev

cleanup_time:
	@set -e; \
	rm -f package-lock.yml && dbt clean && dbt deps

deploy_github_actions:
	@set -e; \
	dbt run -s livequery_base.deploy.marketplace.github --vars '{"UPDATE_UDFS_AND_SPS":True}' -t $(DBT_TARGET); \
	dbt run -m "fsc_evm,tag:gha_tasks" --full-refresh -t $(DBT_TARGET); \
	dbt run-operation fsc_evm.create_gha_tasks --vars '{"START_GHA_TASKS":False}' -t $(DBT_TARGET)

deploy_new_github_action:
	@set -e; \
	dbt run-operation fsc_evm.drop_github_actions_schema -t $(DBT_TARGET); \
	dbt run -m "fsc_evm,tag:gha_tasks" --full-refresh -t $(DBT_TARGET); \
	dbt run-operation fsc_evm.create_gha_tasks --vars '{"START_GHA_TASKS":False}' -t $(DBT_TARGET)

deploy_livequery:
	@set -e; \
	dbt run-operation fsc_evm.drop_livequery_schemas --vars '{"UPDATE_UDFS_AND_SPS": true}' -t $(DBT_TARGET); \
	dbt run -m livequery_base.deploy.core --vars '{"UPDATE_UDFS_AND_SPS": true}' -t $(DBT_TARGET); \
	dbt run-operation fsc_evm.livequery_grants --vars '{"UPDATE_UDFS_AND_SPS": true}' -t $(DBT_TARGET)

deploy_chain_phase_1:
	@set -e; \
	read -p "Exclude receipts_by_hash? [y/n] " receipts_by_hash; \
	dbt run -m livequery_base.deploy.core --vars '{"UPDATE_UDFS_AND_SPS": true}' -t $(DBT_TARGET); \
	dbt run-operation fsc_evm.livequery_grants --vars '{"UPDATE_UDFS_AND_SPS": true}' -t $(DBT_TARGET); \
	dbt run-operation fsc_evm.create_evm_streamline_udfs --vars '{"UPDATE_UDFS_AND_SPS": true}' -t $(DBT_TARGET); \
	dbt run-operation fsc_evm.call_sample_rpc_node -t $(DBT_TARGET); \
	if [ "$(DBT_TARGET)" != "prod" ]; then \
		if [ "$$receipts_by_hash" = "n" ]; then \
			dbt run -m "fsc_evm,tag:phase_1" --exclude "fsc_evm,tag:receipts" --full-refresh --vars '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":true, "MAIN_SL_NEW_BUILD_ENABLED": true, "GLOBAL_STREAMLINE_FR_ENABLED": true}' -t $(DBT_TARGET); \
			dbt test -m "fsc_evm,tag:chainhead"; \
			dbt run -m "fsc_evm,tag:streamline,tag:core,tag:complete" "fsc_evm,tag:streamline,tag:core,tag:realtime" --exclude "fsc_evm,tag:receipts" "fsc_evm,tag:confirm_blocks" --vars '{"MAIN_SL_NEW_BUILD_ENABLED": true, "STREAMLINE_INVOKE_STREAMS":True, "MAIN_SL_TESTING_LIMIT": 500}' -t $(DBT_TARGET); \
		else \
			dbt run -m "fsc_evm,tag:phase_1" --exclude "fsc_evm,tag:receipts_by_hash" --full-refresh --vars '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":true, "MAIN_SL_NEW_BUILD_ENABLED": true, "GLOBAL_STREAMLINE_FR_ENABLED": true}' -t $(DBT_TARGET); \
			dbt test -m "fsc_evm,tag:chainhead"; \
			dbt run -m "fsc_evm,tag:streamline,tag:core,tag:complete" "fsc_evm,tag:streamline,tag:core,tag:realtime" --exclude "fsc_evm,tag:receipts_by_hash" "fsc_evm,tag:confirm_blocks" --vars '{"MAIN_SL_NEW_BUILD_ENABLED": true, "STREAMLINE_INVOKE_STREAMS":True, "MAIN_SL_TESTING_LIMIT": 500}' -t $(DBT_TARGET); \
		fi; \
	else \
		if [ "$$receipts_by_hash" = "n" ]; then \
			dbt run -m "fsc_evm,tag:phase_1" --exclude "fsc_evm,tag:receipts" --full-refresh --vars '{"MAIN_SL_NEW_BUILD_ENABLED": true, "GLOBAL_STREAMLINE_FR_ENABLED": true}' -t $(DBT_TARGET); \
			dbt test -m "fsc_evm,tag:chainhead"; \
			dbt run -m "fsc_evm,tag:streamline,tag:core,tag:complete" "fsc_evm,tag:streamline,tag:core,tag:realtime" --exclude "fsc_evm,tag:receipts" "fsc_evm,tag:confirm_blocks" --vars '{"MAIN_SL_NEW_BUILD_ENABLED": true, "STREAMLINE_INVOKE_STREAMS":True}' -t $(DBT_TARGET); \
		else \
			dbt run -m "fsc_evm,tag:phase_1" --exclude "fsc_evm,tag:receipts_by_hash" --full-refresh --vars '{"MAIN_SL_NEW_BUILD_ENABLED": true, "GLOBAL_STREAMLINE_FR_ENABLED": true}' -t $(DBT_TARGET); \
			dbt test -m "fsc_evm,tag:chainhead"; \
			dbt run -m "fsc_evm,tag:streamline,tag:core,tag:complete" "fsc_evm,tag:streamline,tag:core,tag:realtime" --exclude "fsc_evm,tag:receipts_by_hash" "fsc_evm,tag:confirm_blocks" --vars '{"MAIN_SL_NEW_BUILD_ENABLED": true, "STREAMLINE_INVOKE_STREAMS":True}' -t $(DBT_TARGET); \
		fi; \
	fi; \
	echo "# wait ~10 minutes"; \
	echo "# run deploy_chain_phase_2"

deploy_chain_phase_2:
	@set -e; \
	if [ "$(DBT_TARGET)" != "prod" ]; then \
		dbt run -m "fsc_evm,tag:phase_2" --full-refresh --vars '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":true, "GLOBAL_STREAMLINE_FR_ENABLED": true, "GLOBAL_BRONZE_FR_ENABLED": true, "GLOBAL_SILVER_FR_ENABLED": true, "GLOBAL_GOLD_FR_ENABLED": true, "GLOBAL_NEW_BUILD_ENABLED": true}' -t $(DBT_TARGET); \
		dbt run -m "fsc_evm,tag:streamline,tag:abis,tag:realtime" "fsc_evm,tag:streamline,tag:abis,tag:complete" --vars '{"STREAMLINE_INVOKE_STREAMS":True, "DECODER_SL_NEW_BUILD_ENABLED": true}' -t $(DBT_TARGET); \
	else \
		dbt run -m "fsc_evm,tag:phase_2" --full-refresh --vars '{"GLOBAL_STREAMLINE_FR_ENABLED": true, "GLOBAL_BRONZE_FR_ENABLED": true, "GLOBAL_SILVER_FR_ENABLED": true, "GLOBAL_GOLD_FR_ENABLED": true, "GLOBAL_NEW_BUILD_ENABLED": true}' -t $(DBT_TARGET); \
		dbt run -m "fsc_evm,tag:streamline,tag:abis,tag:realtime" "fsc_evm,tag:streamline,tag:abis,tag:complete" --vars '{"STREAMLINE_INVOKE_STREAMS":True, "DECODER_SL_NEW_BUILD_ENABLED": true}' -t $(DBT_TARGET); \
		make deploy_github_actions DBT_TARGET=$(DBT_TARGET); \
	fi; \
	echo "# tasks set to SUSPEND by default"; \
	echo "# kick dbt_alter_gha_task or dbt_alter_all_gha_tasks workflow to RESUME tasks, where applicable"; \
	echo "# wait ~10 minutes"; \
	echo "# run deploy_chain_phase_3"

deploy_chain_phase_3:
	@set -e; \
	if [ "$(DBT_TARGET)" != "prod" ]; then \
		dbt run -m "fsc_evm,tag:phase_2" --vars '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":true}' -t $(DBT_TARGET); \
		dbt run -m "fsc_evm,tag:phase_3" --full-refresh --vars '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":true, "GLOBAL_STREAMLINE_FR_ENABLED": true, "GLOBAL_SILVER_FR_ENABLED": true, "GLOBAL_GOLD_FR_ENABLED": true, "GLOBAL_NEW_BUILD_ENABLED": true}' -t $(DBT_TARGET); \
		dbt run -m "fsc_evm,tag:silver,tag:abis" "fsc_evm,tag:streamline,tag:decoded_logs,tag:realtime" "fsc_evm,tag:streamline,tag:decoded_logs,tag:complete" --vars '{"STREAMLINE_INVOKE_STREAMS":True, "DECODER_SL_TESTING_LIMIT": 500}' -t $(DBT_TARGET); \
	else \
		dbt run -m "fsc_evm,tag:phase_2" -t $(DBT_TARGET); \
		dbt run -m "fsc_evm,tag:phase_3" --full-refresh --vars '{"GLOBAL_STREAMLINE_FR_ENABLED": true, "GLOBAL_SILVER_FR_ENABLED": true, "GLOBAL_GOLD_FR_ENABLED": true, "GLOBAL_NEW_BUILD_ENABLED": true}' -t $(DBT_TARGET); \
		dbt run -m "fsc_evm,tag:silver,tag:abis" "fsc_evm,tag:streamline,tag:decoded_logs,tag:realtime" "fsc_evm,tag:streamline,tag:decoded_logs,tag:complete" --vars '{"STREAMLINE_INVOKE_STREAMS":True}' -t $(DBT_TARGET); \
	fi; \
	echo "# tasks set to SUSPEND by default"; \
	echo "# kick dbt_alter_gha_task or dbt_alter_all_gha_tasks workflow to RESUME tasks, where applicable"; \
	echo "# wait ~10 minutes"; \
	echo "# run deploy_chain_phase_4"

deploy_chain_phase_4:
	@set -e; \
	if [ "$(DBT_TARGET)" != "prod" ]; then \
		dbt run -m "fsc_evm,tag:phase_3" --vars '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":true}' -t $(DBT_TARGET); \
		dbt run -m "fsc_evm,tag:phase_4" --full-refresh -t $(DBT_TARGET); \
	else \
		dbt run -m "fsc_evm,tag:phase_3" -t $(DBT_TARGET); \
		dbt run -m "fsc_evm,tag:phase_4" --full-refresh -t $(DBT_TARGET); \
	fi; \
	echo "# tasks set to SUSPEND by default"; \
	echo "# kick dbt_alter_gha_task or dbt_alter_all_gha_tasks workflow to RESUME tasks, where applicable"

.PHONY: deploy_github_actions cleanup_time deploy_new_github_action deploy_chain_phase_1 deploy_chain_phase_2 deploy_chain_phase_3 deploy_livequery deploy_chain_phase_4