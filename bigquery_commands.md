Creating an external table from parquet file stored in BigQuery
```sql
CREATE OR REPLACE EXTERNAL TABLE dtc-de-course-388001.onepiece.raw_data
WITH PARTITION COLUMNS
OPTIONS (
format = 'PARQUET',
uris = ['gs://onepiece-dtc-de-course-388001/onepieceanime/outputs/raw_data/*'],
hive_partition_uri_prefix = 'gs://onepiece-dtc-de-course-388001/onepieceanime/outputs/raw_data/',
require_hive_partition_filter = false
);
```
![External table creation](./images/external_table_creation.png)


Record count
```sql
select count(1) as no_records 
from onepiece.raw_data;
```
![Record count](./images/record_count.png)

Sample records
```sql
select *
from onepiece.raw_data
where start = 2004;
```
![Sample records](./images/sample_data_screenshot.png)
