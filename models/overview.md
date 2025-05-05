{% docs __overview__ %}

# Welcome to the Flipside Crypto Optimism Models Documentation!

## **What does this documentation cover?**
The documentation included here details the design of the Optimism tables and views available via [Flipside Crypto.](https://flipsidecrypto.xyz/) For more information on how these models are built, please see [the github repository.](https://github.com/FlipsideCrypto/optimism-models)

## **How do I use these docs?**
The easiest way to navigate this documentation is to use the Quick Links below. These links will take you to the documentation for each table, which contains a description, a list of the columns, and other helpful information.

If you are experienced with dbt docs, feel free to use the sidebar to navigate the documentation, as well as explore the relationships between tables and the logic building them.

There is more information on how to use dbt docs in the last section of this document.

## **Quick Links to Table Documentation**

**Click on the links below to jump to the documentation for each schema.**

### Core Tables

**Dimensional Tables**
- [dim_contracts](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.core__dim_contracts)
- [dim_contract_abis](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.core__dim_contract_abis)
- [dim_labels](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.core__dim_labels)

**Fact Tables:**
- [fact_blocks](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.core__fact_blocks)
- [fact_transactions](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.core__fact_transactions)
- [fact_event_logs](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.core__fact_event_logs)
- [fact_traces](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.core__fact_traces)

**Convenience Tables:**
- [ez_decoded_event_logs](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.core__ez_decoded_event_logs)
- [ez_native_transfers](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.core__ez_native_transfers)
- [ez_token_transfers](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.core__ez_token_transfers)

### Price Tables (optimism.price)
- [dim_asset_metadata](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.price__dim_asset_metadata)
- [fact_prices_ohlc_hourly](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.price__fact_prices_ohlc_hourly)
- [ez_asset_metadata](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.price__ez_asset_metadata)
- [ez_prices_hourly](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.price__ez_prices_hourly)

### DeFi Tables (optimism.defi)
- [ez_bridge_activity](https://flipsidecrypto.github.io/optimism-models/#!/model/model.optimism_models.defi__ez_bridge_activity)
- [ez_dex_swaps](https://flipsidecrypto.github.io/optimism-models/#!/model/model.optimism_models.defi__ez_dex_swaps)
- [dim_dex_liquidity_pools](https://flipsidecrypto.github.io/optimism-models/#!/model/model.optimism_models.defi__dim_dex_liquidity_pools)
- [ez_lending_borrows](https://flipsidecrypto.github.io/optimism-models/#!/model/model.optimism_models.defi__ez_lending_borrows) 
- [ez_lending_deposits](https://flipsidecrypto.github.io/optimism-models/#!/model/model.optimism_models.defi__ez_lending_deposits)
- [ez_lending_flashloans](https://flipsidecrypto.github.io/optimism-models/#!/model/model.optimism_models.defi__ez_lending_flashloans)
- [ez_lending_liquidations](https://flipsidecrypto.github.io/optimism-models/#!/model/model.optimism_models.defi__ez_lending_liquidations)
- [ez_lending_repayments](https://flipsidecrypto.github.io/optimism-models/#!/model/model.optimism_models.defi__ez_lending_repayments)
- [ez_lending_withdraws](https://flipsidecrypto.github.io/optimism-models/#!/model/model.optimism_models.defi__ez_lending_withdraws)

### Flipside Partner Tables (optimism.partner_name)

**Vertex Tables:**
- [dim_products](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.vertex__dim_products)
- [ez_clearing_house_events](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.vertex__ez_clearing_house_events)
- [ez_liquidations](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.vertex__ez_liquidations)
- [ez_perp_trades](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.vertex__ez_perp_trades)
- [ez_spot_trades](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.vertex__ez_spot_trades)
- [ez_account_stats](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.vertex__ez_account_stats)
- [ez_edge_trades](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.vertex__ez_edge_trades)
- [ez_market_depth_stats](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.vertex__ez_market_depth_stats)
- [ez_market_stats](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.vertex__ez_market_stats)
- [ez_money_markets](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.vertex__ez_money_markets)

### NFT Tables (optimism.nft)
- [ez_nft_transfers](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.nft__ez_nft_transfers)
- [ez_nft_sales](https://flipsidecrypto.github.io/optimism-models/#!/model/model.optimism_models.nft__ez_nft_sales)

### Stats Tables (optimism.stats)
- [ez_core_metrics_hourly](https://flipsidecrypto.github.io/optimism-models/#!/model/model.fsc_evm.stats__ez_core_metrics_hourly)

## **Helpful User-Defined Functions (UDFs)**

UDFs are custom functions built by the Flipside team that can be used in your queries to make your life easier. 

Please visit [LiveQuery Functions Overview](https://flipsidecrypto.github.io/livequery-models/#!/overview) for a full list of helpful UDFs.

## **Data Model Overview**

The Optimism models are built a few different ways, but the core fact tables are built using three layers of sql models: **bronze, silver, and gold (or core).**

- Bronze: Data is loaded in from the source as a view
- Silver: All necessary parsing, filtering, de-duping, and other transformations are done here
- Gold (or core): Final views and tables that are available publicly

The dimension tables are sourced from a variety of on-chain and off-chain sources.

Convenience views (denoted ez_) are a combination of different fact and dimension tables. These views are built to make it easier to query the data.

NOTE: Optimism is currently operating in it's Testnet phase. Flipside will provide Mainnet data tables once Optimism Mainnet is deployed. 

## **Using dbt docs**
### Navigation

You can use the ```Project``` and ```Database``` navigation tabs on the left side of the window to explore the models in the project.

### Database Tab

This view shows relations (tables and views) grouped into database schemas. Note that ephemeral models are *not* shown in this interface, as they do not exist in the dataoptimism.

### Graph Exploration

You can click the blue icon on the bottom-right corner of the page to view the lineage graph of your models.

On model pages, you'll see the immediate parents and children of the model you're exploring. By clicking the Expand button at the top-right of this lineage pane, you'll be able to see all of the models that are used to build, or are built from, the model you're exploring.

Once expanded, you'll be able to use the ```--models``` and ```--exclude``` model selection syntax to filter the models in the graph. For more information on model selection, check out the [dbt docs](https://docs.getdbt.com/docs/model-selection-syntax).

Note that you can also right-click on models to interactively filter and explore the graph.


### **More information**
- [Flipside](https://flipsidecrypto.xyz/)
- [Velocity](https://app.flipsidecrypto.com/velocity?nav=Discover)
- [Tutorials](https://docs.flipsidecrypto.com/our-data/tutorials)
- [Github](https://github.com/FlipsideCrypto/optimism-models)
- [What is dbt?](https://docs.getdbt.com/docs/introduction)


{% enddocs %}