Creating a Dataproc cluster with idle TTL 5 min (optimize cost)
```sh
gcloud dataproc clusters create onepiece-cluster --region=us-central1 --zone=us-central1-a --max-idle=5m
```


Dataproc cluster job submit
```sh
gcloud dataproc jobs submit pyspark \
    --cluster=onepiece-cluster \
    --region=us-central1 \
    gs://onepiece-dtc-de-course-388001/onepieceanime/script/analysis_file.py \
    -- \
        --input_file gs://onepiece-dtc-de-course-388001/onepieceanime/input/onepiece.csv \
        --output_folder gs://onepiece-dtc-de-course-388001/onepieceanime/outputs/
```

### Load the tables as external tables to query from BigQuery

Creating a dataset named onepiece
```sh
bq mk --location=us-central1 onepiece 
```

# Below loads as a native table, however we will load as an external table
```sh
bq load \
    --source_format=PARQUET \
    --hive_partitioning_mode=AUTO \
    --hive_partitioning_source_uri_prefix=gs://onepiece-dtc-de-course-388001/onepieceanime/outputs/raw_data/ \
    onepiece.raw_data \
    gs://onepiece-dtc-de-course-388001/onepieceanime/outputs/raw_data/*
```
