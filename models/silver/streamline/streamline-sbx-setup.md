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

ALTER USER AWS_LAMBDA_OPTIMISM_API_SBX SET ROLE AWS_LAMBDA_OPTIMISM_API_SBX;

-- Note that the password must meet Snowflake's password requirements, which include a minimum length of 8 characters, at least one uppercase letter, at least one lowercase letter, and at least one number or special character.

ALTER USER AWS_LAMBDA_OPTIMISM_API_SBX SET PASSWORD = 'new_password';
```
### Register Snowflake integration and UDF's

- Register the ![snowflake api integration](/macros/streamline/api_integrations.sql) either manually on `snowsight worksheet` or via `dbt`

```sql
-- Manually run on snowflake
CREATE api integration IF NOT EXISTS aws_optimism_api_sbx_shah api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::579011195466:role/snowflake-api-optimism' api_allowed_prefixes = (
            'https://3ifufl19z4.execute-api.us-east-1.amazonaws.com/sbx/'
        ) enabled = TRUE;
```

```zsh
# Use dbt to run create_aws_terra_api macro
dbt run-operation create_aws_optimism_api --target dev
```

- Add the UDF to the ![create udfs macro](/macros/create_udfs.sql)
- Register UDF

```sql
CREATE
OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_json_rpc(json variant) returns text api_integration = aws_terra_api_sbx_shah AS 'https://33fgv8p4d4.execute-api.us-east-1.amazonaws.com/sbx/udf_bulk_json_rpc';
```

- Add the ![_max_block_by_date.sql](_max_block_by_date.sql) model
- Add the ![streamline__blocks](streamline__blocks.sql) model
- Add the ![get_base_table_udft.sql](../.././macros/streamline/get_base_table_udft.sql) macro

- Grant privileges to `AWS_LAMBDA_TERRA_API`

```sql
GRANT SELECT ON VIEW streamline.pc_getBlock_realtime TO ROLE AWS_LAMBDA_TERRA_API;

GRANT USAGE ON DATABASE OPTIMISM_DEV TO ROLE AWS_LAMBDA_TERRA_API;
```

```zsh
dbt run --vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' -m 1+models/silver/streamline/core/realtime/streamline__pc_getBlock_realtime.sql --profile terra --target sbx --profiles-dir ~/.dbt
```
