## Sandbox integration setup

In order to perform a `sandbox` `streamline` integration you need to ![register](../../../macros/streamline/api_integrations.sql) with your `sbx api gateway` endpoint. 

### DBT Global config
- The first step is to configure your `global dbt` profile:

```zsh
# create dbt global config
touch ~/.dbt/profiles.yaml 
```

- And add the following into `~/.dbt/profiles.yaml`

```yaml
optimism:
  target: sbx
  outputs:
    sbx:
      type: snowflake
      account: vna27887.us-east-1
      role: DBT_CLOUD_OPTIMISM 
      user: <REPLACE_WIHT_YOUR_USER>@flipsidecrypto.com
      authenticator: externalbrowser
      region: us-east-1
      database: OPTIMISM_DEV
      warehouse: DBT
      schema: STREAMLINE
      threads: 12
      client_session_keep_alive: False
      query_tag: dbt_<REPLACE_WITH_YOUR_USER>_dev
```

### Create user & role for streamline lambdas to use and apply the appropriate roles

```sql
-- Create OPTIMISM_DEV.streamline schema  
CREATE SCHEMA OPTIMISM_DEV.STREAMLINE

CREATE ROLE AWS_LAMBDA_OPTIMISM_API_SBX;

CREATE USER AWS_LAMBDA_OPTIMISM_API_SBX PASSWORD='abc123' DEFAULT_ROLE = AWS_LAMBDA_OPTIMISM_API_SBX MUST_CHANGE_PASSWORD = TRUE;

GRANT SELECT ON ALL VIEWS IN SCHEMA OPTIMISM_DEV.STREAMLINE TO ROLE AWS_LAMBDA_OPTIMISM_API_SBX;

GRANT ROLE AWS_LAMBDA_OPTIMISM_API TO USER AWS_LAMBDA_OPTIMISM_API;

-- Note that the password must meet Snowflake's password requirements, which include a minimum length of 8 characters, at least one uppercase letter, at least one lowercase letter, and at least one number or special character.

ALTER USER AWS_LAMBDA_OPTIMISM_API_SBX SET PASSWORD = 'new_password';
```
### Register Snowflake integration and UDF's

- Register the ![snowflake api integration](../../../macros/streamline/api_integrations.sql) either manually on `snowsight worksheet` or via `dbt`

```sql
-- Manually run on snowflake
CREATE api integration IF NOT EXISTS aws_optimism_api_sbx_shah api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::579011195466:role/snowflake-api-optimism' api_allowed_prefixes = (
            'https://3ifufl19z4.execute-api.us-east-1.amazonaws.com/sbx/'
        ) enabled = TRUE;
```

```zsh
# Use dbt to run create_aws_optimism_api macro
dbt run-operation create_aws_optimism_api --target dev
```

- Add the UDF to the ![create udfs macro](/macros/create_udfs.sql)
- Register UDF

```sql
CREATE
OR REPLACE EXTERNAL FUNCTION streamline.udf_get_chainhead() returns text api_integration = aws_optimism_api_sbx_shah AS 'https://3ifufl19z4.execute-api.us-east-1.amazonaws.com/sbx/udf_bulk_json_rpc';

CREATE
OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_json_rpc(json variant) returns text api_integration = aws_optimism_api_sbx_shah AS 'https://3ifufl19z4.execute-api.us-east-1.amazonaws.com/sbx/bulk_decode_logs';

GRANT USAGE ON FUNCTION streamline.udf_get_chainhead() TO DBT_CLOUD_OPTIMISM;
GRANT USAGE ON FUNCTION streamline.udf_bulk_json_rpc(variantq) TO DBT_CLOUD_OPTIMISM;
GRANT USAGE ON FUNCTION streamline.udtf_get_base_table(integer) TO DBT_CLOUD_OPTIMISM;
```

- Add the ![_max_block_by_date.sql](_max_block_by_date.sql) model
- Add the ![streamline__blocks](streamline__blocks.sql) model
- Add the ![get_base_table_udft.sql](../.././macros/streamline/get_base_table_udft.sql) macro

- Grant privileges to `AWS_LAMBDA_OPTIMISMT_API`

```sql
GRANT USAGE ON DATABASE OPTIMISM_DEV TO ROLE AWS_LAMBDA_OPTIMISM_API;
GRANT USAGE ON SCHEMA STREAMLINE TO ROLE AWS_LAMBDA_OPTIMISM_API;
GRANT USAGE ON WAREHOUSE DBT_CLOUD TO ROLE AWS_LAMBDA_OPTIMISM_API;
```

## Run decode models

```zsh
# SBX
dbt run --vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' -m 1+models/silver/streamline/history --profile optimism --target sbx --profiles-dir ~/.dbt

# DEV
dbt run --vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' -m 1+models/silver/streamline/history/ --profile optimism --target dev --profiles-dir ~/.dbt

# PROD
dbt run --vars '{"STREAMLINE_INVOKE_STREAMS":True}' -m 1+models/silver/streamline/history/ --profile optimism --target prod --profiles-dir ~/.dbt
```
