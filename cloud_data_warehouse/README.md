## Project

The user activity and songs metadata are stored as json files in AWS S3. The primary goal of this project is to build an efficient ETL (Extract, Transform, Load) pipeline. This pipeline will extract the data from AWS S3, stage it in AWS Redshift, and transform it into a set of tables (fact and dimensional) in star schema. These tables will empower the analytics team to continue discovering valuable insights regarding the songs that Sparkify's users are listening to.

## Project Structure

```
├── cloud_data_warehouse
│   ├── analyse.py          # Script to analyse data.
│   ├── create_tables.py    # Create tables in redShift
│   ├── data                # Data directory
│   │   ├── log_data_sample.json    # Downloaded log_sample data.
│   │   ├── log_json_path.json      # Downloaded log_meta data.
│   │   └── song_data_sample.json   # Downloaded songs data.
│   ├── download_datasets.py      # Script to download data.
│   ├── dwh.cfg                   # config file (not committed)
│   ├── dwh.cfg.example           # config file example.
│   ├── etl.py                    # ETL script to load and ingest data.
│   ├── __init__.py
│   ├── README.md                 # this file
│   ├── setup_aws_services.py     # Script to provision AWS services
│   ├── sql_queries.py            # SQL Queries to create/read tables/data.
│   └── tear_down_aws_services.py   # Script to free up AWS services.
└── requirements.txt                    # PIP dependencies.
```

## Prepare Env

1. `pip install -r requirements.txt`
2. `cp dwh.cfg.example dwh.cfg` and fill appropriate values.

## Execute

1. `python3 download_datasets.py`

This will download sample dataset files in the `data` directory.

2. `python3 setup_aws_services.py`

This will create `IAM`, `RedShift Cluster` and setup `Security Groups` as required.

3. `python3 create_tables.py`

This will create tables in RedShift cluster.

4. `python3 etl.py`

This will copy data from `S3 bucket` to the staging tables by `COPY` command.

5. `python3 analyse.py`

To list number of rows, five most played articles etc.

6. `python3 tear_down_aws_services.py`

This will free up created AWS services.
