 select * from OPTIMISM.not_null_silver.exactly_liquidations_COLLATERAL_SYMBOL;

 select 
    * 
 from 
    OPTIMISM.silver.contracts 
 where 
    contract_address = '0x6926b434cce9b5b7966ae1bfeef6d0a7dcf3a8bb';

select * from OPTIMISM_dev.silver.exactly_asset_details;

select * from OPTIMISM.silver.traces where tx_hash = '0x441c2c5cb943d43e8685d8f3d43f62b62912126de9f1ed1457609007f7abf6a7';
select * from OPTIMISM.silver.traces where tx_hash = '0xc1a5ace2b5433bf9bb66f6d2eed00c9f5229ff386f0898500ab59282f9e6e0e1';
select count(*) 
from OPTIMISM.silver.logs 
where contract_address = '0x6926b434cce9b5b7966ae1bfeef6d0a7dcf3a8bb';

select * from crosschain.silver.function_sig limit 10;