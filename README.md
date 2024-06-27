# This repo is for OTUS MLOPs course homework 

## The structure of the repo is

- notebooks

    - chaos

    - reports
- src
    - scripts
    - utils
- static




# TASK 2
## Here is s3 bucket for the home task.
`s3://otus-task-n2/`

## Here is the path to Jupyter notebook with s3loader code and extracted script.
`./notebooks/chaos/load_to_s3.ipynb`
`./src/scripts/s3loader.py`

![S3 Bucket](/static/screenshots/s3bucket.JPG)

## And this screenshot shows the copy of data in hdfs 
![data it hdfs](/static/screenshots/hdfs-data.JPG)


# TASK 3
## S3 bucket with the prepocessed data
`s3://otus-task-n3/`

## The data preprocessing is described in Jupyter notebook
`./notebooks/reports/fraud_data_cleaing.ipynb`
## Here is the path to the data prepocessing / cleaning script made from the prev notebook.
`./src/scripts/clean_fraud_data.py`


# TASK 4
## 3 successful DAG runs in a row
![data it hdfs](/static/screenshots/successful_dag.PNG)

## VMs for dataproc
![data it hdfs](/static/screenshots/dataproc_airflow.JPG)

## DataProc for AirFlow
![data it hdfs](/static/screenshots/dataproc_airflow2.JPG)


# TASK 5
## 3 successful DAG runs in a row with model training. Although I skipped data preprocessing to save some time.
![data it hdfs](/static/screenshots/airflow-training.JPG)

## All runs in MLFLOW - last several ones arе successful 
![data it hdfs](/static/screenshots/mlflow-training.JPG)

## Metrics in MLFlow for DAG task
![data it hdfs](/static/screenshots/mlflow-training1.JPG)
## Metrics in MLFlow for manual training script execution 
![data it hdfs](/static/screenshots/mlflow-training2.JPG)
## Model artifacts in mlflow
![data it hdfs](/static/screenshots/mlflow-training3.JPG)
## Model artifacts in S3
![data it hdfs](/static/screenshots/mlflow-training-artifacts.JPG)
