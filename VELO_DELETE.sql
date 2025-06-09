SELECT 
'optimism_dev' as database_name,
'DROP VIEW ' || upper(database_name) || '.' || table_schema || '.' || table_name || ';' as drop_statement
FROM optimism_dev.information_schema.views 
WHERE table_schema = 'STREAMLINE'
AND table_name LIKE 'VELODROME%'
AND table_name NOT IN ('VELODROME_SWAPS', 'VELODROME_POOLS')
order by 1 asc;
