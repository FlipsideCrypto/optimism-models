{% docs __overview__ %}

# Welcome to the Flipside Crypto Optimism Models Documentation!

# NOTE: Data is in 'lite mode' - meaning, histrorical data has not yet been backfilled. Please see `min(block_timestamp)`

## **What is Flipside?**

[Flipside Crypto](https://flipsidecrypto.xyz/earn) provides Community Enabled Crypto Analytics, allowing our users to create and share data insights on the crypto projects they care most about.

**Flipside Crypto puts pre-modeled and labeled blockchain data in the hands of communities.**

Through dashboard and visualization tools, as well as auto-generated API endpoints, data analysts can easily create queries that answer any question via a tool called [Velocity](https://app.flipsidecrypto.com/velocity?nav=Discover).

**Community members earn bounties for answering questions with data.**

Bounties provide incentive and direction, so crypto projects can quickly source the data insights they need in order to grow.

**Flipside works directly with leading crypto projects to reward on-demand analytics through structured bounty programs.**

Questions sourced directly from the community provide insight into what communities care about as well as analytics needed to drive ecosystem engagement and growth.

## **What does this documentation cover?**
The documentation included here details the design of the Optimism tables and views available via [Flipside Crypto.](https://flipsidecrypto.xyz/earn) For more information on how these models are built, please see [the github repository.](https://github.com/FlipsideCrypto/optimism-models)

### **Quick Links to Table Documentation**

- [fact_blocks](https://flipsidecrypto.github.io/optimism-models/#!/model/model.optimism_models.core__fact_blocks)
- [fact_event_logs](https://flipsidecrypto.github.io/optimism-models/#!/model/model.optimism_models.core__fact_event_logs)
- [fact_traces](https://flipsidecrypto.github.io/optimism-models/#!/model/model.optimism_models.core__fact_traces)
- [fact_transactions](https://flipsidecrypto.github.io/optimism-models/#!/model/model.optimism_models.core__fact_transactions)
- [dim_lables](https://flipsidecrypto.github.io/optimism-models/#!/model/model.optimism_models.core__dim_labels)

## **Data Model Overview**

The Optimism models are built a few different ways, but the core fact table are built using three layers of sql models: **bronze, silver, and gold (or core).**

- Bronze: Data is loaded in from the source as a view
- Silver: All necessary parsing, filtering, de-duping, and other transformations are done here
- Gold (or core): Final views and tables that are available in Velocity

The dimension tables are sourced from a variety of on-chain and off-chain sources.

Convenience views (denoted ez_) are a combination of different fact and dimension tables. 

A user-defined-function (UDF) is available to decode hex encoded values to integers in this database. You can call this UDF by using `optimism.public.udf_hex_to_int(FIELD)`.


## **Using dbt docs**
### Navigation

You can use the ```Project``` and ```Database``` navigation tabs on the left side of the window to explore the models in the project.

### Database Tab

This view shows relations (tables and views) grouped into database schemas. Note that ephemeral models are *not* shown in this interface, as they do not exist in the database.

### Graph Exploration

You can click the blue icon on the bottom-right corner of the page to view the lineage graph of your models.

On model pages, you'll see the immediate parents and children of the model you're exploring. By clicking the Expand button at the top-right of this lineage pane, you'll be able to see all of the models that are used to build, or are built from, the model you're exploring.

Once expanded, you'll be able to use the ```--models``` and ```--exclude``` model selection syntax to filter the models in the graph. For more information on model selection, check out the [dbt docs](https://docs.getdbt.com/docs/model-selection-syntax).

Note that you can also right-click on models to interactively filter and explore the graph.


### **More information**
- [Flipside](https://flipsidecrypto.xyz/earn)
- [Velocity](https://app.flipsidecrypto.com/velocity?nav=Discover)
- [Tutorials](https://docs.flipsidecrypto.com/our-data/tutorials)
- [Github](https://github.com/FlipsideCrypto/optimism-models)
- [Query Editor Shortcuts](https://docs.flipsidecrypto.com/velocity/query-editor-shortcuts)
- [What is dbt?](https://docs.getdbt.com/docs/introduction)



{% enddocs %}
