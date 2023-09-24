This repo is to analyse OnePiece anime, popularity of its episodes, put into GCS (along with a couple of summarized outputs) and BigQuery for general availability
Dataset is picked from [Kaggle(]https://www.kaggle.com/datasets/aditya2803/one-piece-anime)

### Tech used:
- _Google Cloud Platform_
- _PySpark_
- _GCS_ (to store input and output files)
    - _Python SDK_ to create bucket (if not exists) and upload the input file 
- _Google Cloud DataProc_ (to submit PySpark job)
- _BigQuery_
- _Airflow_ (to seamlessly run dependencies)
- _Looker Studio_ (for visualization)

### Summarized outputs (via PySpark)
- Year-wise summary
- Overall summary for the show
- Actual data in summarized format


- [Todos](/todos.md)
- [Questions to answer](/questions.md)
- [Gcloud commands](/gcloud_commands.md)


