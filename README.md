## Profile Set Up

#### Use the following within profiles.yml 
----

```yml
optimism:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <ACCOUNT>
      role: <ROLE>
      user: <USERNAME>
      password: <PASSWORD>
      region: <REGION>
      database: OPTIMISM_DEV
      warehouse: <WAREHOUSE>
      schema: silver
      threads: 12
      client_session_keep_alive: False
      query_tag: <TAG>
    prod:
      type: snowflake
      account: <ACCOUNT>
      role: <ROLE>
      user: <USERNAME>
      password: <PASSWORD>
      region: <REGION>
      database: OPTIMISM
      warehouse: <WAREHOUSE>
      schema: silver
      threads: 12
      client_session_keep_alive: False
      query_tag: <TAG>
```

### Variables

To control the creation of UDF or SP macros with dbt run:
* UPDATE_UDFS_AND_SPS
When True, executes all macros included in the on-run-start hooks within dbt_project.yml on model run as normal
When False, none of the on-run-start macros are executed on model run

Default values are False

* Usage:
dbt run --var '{"UPDATE_UDFS_AND_SPS":True}'  -m ...

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
