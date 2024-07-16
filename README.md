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
![successful_dag](/static/screenshots/successful_dag.PNG)

## VMs for dataproc
![dataproc_airflow](/static/screenshots/dataproc_airflow.JPG)

## DataProc for AirFlow
![dataproc_airflow2](/static/screenshots/dataproc_airflow2.JPG)


# TASK 5
## 3 successful DAG runs in a row with model training. Although I skipped data preprocessing to save some time.
![airflow-training](/static/screenshots/airflow-training.JPG)

## All runs in MLFLOW - last several ones arе successful 
![All runs in MLFLOW](/static/screenshots/mlflow-training.JPG)

## Metrics in MLFlow for DAG task
![metrics for dag](/static/screenshots/mlflow-training1.JPG)
## Metrics in MLFlow for manual training script execution 
![metrcis for manual](/static/screenshots/mlflow-training2.JPG)
## Model artifacts in mlflow
![model artifacts mlflow](/static/screenshots/mlflow-training3.JPG)
## Model artifacts in S3
![model artifacts s3](/static/screenshots/mlflow-training-artifacts.JPG)

# TASK 6

## I've implemented A/B testing through statistical difference measures for all the metrics of the newly trained model. (the number of bootstap samples is low beacuse of the project's educational purpose - to save some time). The A/B test is conducted within the PySpark training script and is run by an Airflow DAG.  

## Metrics distributions saved in MLFlow (mean, std, confidence interval, p-value, z-score)
![airflow-training](/static/screenshots/hw6-run1-metrics.JPG) ![All runs in MLFLOW](/static/screenshots/hw6-run2-metrics.JPG)

## AB test result for runs 0 - 1 / MLFLow artifacts
![metrics for dag](/static/screenshots/hw6-run1-ab.JPG)
## AB test result for runs 1 - 2 / MLFLow artifacts
![metrics for dag](/static/screenshots/hw6-run2-ab.JPG)

# TASK 7

## I've created Kafka Cluster with user and topic, implemented producer and consumer alongside with model inference.
## To test moderl performance for 10 minutes 10k messages were generated and 4356 trascasrions were processed. So in this setup the model can process ~7 msgs per second.
## The same DAG file as for training were refactored for running inference.py on shedule and storing inference artifacts to S3
## Model for inference is loaded from S3.

## Kafka cluster
![kafka](/static/screenshots/hw7-kafka-cluster.JPG)
![kafka](/static/screenshots/hw7-kafka-hosts.JPG)
![kafka](/static/screenshots/hw7-kafka-topic.JPG)

## Kafka producer
![producer](/static/screenshots/hw7-producer.JPG) 

## Inference + consumer
![consumer + inference](/static/screenshots/hw7-consumer-inference.JPG)

