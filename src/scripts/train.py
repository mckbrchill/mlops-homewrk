import os
import logging
import argparse
from datetime import datetime
import sys
import numpy as np
import boto3
import random
from scipy.stats import norm

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

import mlflow
from mlflow.tracking import MlflowClient


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

os.environ["MLFLOW_S3_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
os.environ["AWS_ACCESS_KEY_ID"] = ""
os.environ["AWS_SECRET_ACCESS_KEY"] = ""


def get_random_filename():

    s3_bucket = 'otus-task-n3'
    
    s3_client = boto3.client(
        's3',
        endpoint_url='https://storage.yandexcloud.net'
    )
    
    paginator = s3_client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=s3_bucket, Delimiter='/')
    
    first_level_keys = []
    
    for response in response_iterator:
        if 'CommonPrefixes' in response:
            for prefix in response['CommonPrefixes']:
                first_level_keys.append(prefix['Prefix'])
    
    txt_files = [x[:-1] for x in first_level_keys if '.txt' in x]
    
    random_file = random.choice(txt_files)
    
    return random_file


def get_dataframe(spark):
    
    # s3_path = 's3a://otus-task-n3/2019-09-21.txt'
    filename = get_random_filename()
    s3_path = f"s3a://otus-task-n3/{filename}"

    df = spark.read.parquet(s3_path, 
                            header=True, 
                            inferSchema=True,
                            **{
                                "key": "",
                                "secret": "",
                                "client_kwargs": {"endpoint_url": "https://storage.yandexcloud.net"}
                            })
    
    return df

def preproc(df):
    
    # Assuming df is DataFrame
    df = df.withColumn('tx_datetime', F.to_timestamp(df['tx_datetime']))
    # Time-based features
    df = df.withColumn('is_weekend', F.when(F.dayofweek(df['tx_datetime']) >= 5, 1).otherwise(0))
    # Sort by customer and transaction datetime
    df = df.orderBy(['customer_id', 'tx_datetime'])
    # Customer behavior features
    window_spec = Window.partitionBy('customer_id').orderBy('tx_datetime')
    # Add a lag column as a timestamp
    df = df.withColumn('lagged_tx_datetime', F.lag('tx_datetime', 1).over(window_spec).cast('timestamp'))
    # Calculate time_since_last_tx in seconds
    df = df.withColumn('time_since_last_tx', 
                       (F.col('tx_datetime').cast('long') - F.col('lagged_tx_datetime').cast('long')) / F.lit(1000))
    # Drop the intermediate lagged_tx_datetime column if not needed
    df = df.drop('lagged_tx_datetime')
    df = df.withColumn('avg_tx_amount_customer', F.avg('tx_amount').over(window_spec))
    df = df.withColumn('tx_count_customer', F.count('tranaction_id').over(window_spec))
    df = df.withColumn('var_tx_amount_customer', F.stddev('tx_amount').over(window_spec))
    # Terminal-based features
    window_spec_terminal = Window.partitionBy('terminal_id').orderBy('tx_datetime')
    df = df.withColumn('avg_tx_amount_terminal', F.avg('tx_amount').over(window_spec_terminal))
    df = df.withColumn('tx_count_terminal', F.count('tranaction_id').over(window_spec_terminal))
    df = df.withColumn('var_tx_amount_terminal', F.stddev('tx_amount').over(window_spec_terminal))    
    # Convert boolean column to binary (1/0)
    df = df.withColumn('is_weekend', F.col('is_weekend').cast('integer'))
    # Drop rows with null values
    df = df.dropna()
    
    return df


def scale(df):
    
    numeric_columns = ['tx_amount', 'time_since_last_tx', 'avg_tx_amount_customer', 'tx_count_customer',
                       'var_tx_amount_customer', 'avg_tx_amount_terminal', 'tx_count_terminal', 'var_tx_amount_terminal']
    
    scaler = StandardScaler(inputCol="scaled_features", outputCol="features")
    assembler = VectorAssembler(inputCols=numeric_columns, outputCol="scaled_features")
    pipeline = Pipeline(stages=[assembler, scaler])
    pipeline_model = pipeline.fit(df)
    df = pipeline_model.transform(df)

    df = df.dropna()
    
    return df


def bootstrap_metrics(test_df, model, num_samples=5):
    evaluator = MulticlassClassificationEvaluator(labelCol='tx_fraud', predictionCol="prediction")
    metrics = {'accuracy': [], 'precision': [], 'recall': [], 'f1': []}
    
    for _ in range(num_samples):
        sample_df = test_df.sample(withReplacement=True, fraction=0.1, seed=np.random.randint(0, 10000))
        predictions = model.transform(sample_df)
        
        metrics['accuracy'].append(evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"}))
        metrics['precision'].append(evaluator.evaluate(predictions, {evaluator.metricName: "weightedPrecision"}))
        metrics['recall'].append(evaluator.evaluate(predictions, {evaluator.metricName: "weightedRecall"}))
        metrics['f1'].append(evaluator.evaluate(predictions, {evaluator.metricName: "f1"}))
    
    return metrics

def calculate_z_test(new_metrics, prev_metrics):
    z_scores = {}
    p_values = {}
    
    for metric in new_metrics:
        new_mean = np.mean(new_metrics[metric])
        new_std = np.std(new_metrics[metric])
        prev_mean = prev_metrics[metric][0]
        prev_std = prev_metrics[metric][1]
        
        z_score = (new_mean - prev_mean) / np.sqrt(new_std**2 + prev_std**2)
        p_value = 2 * (1 - norm.cdf(abs(z_score)))
        
        z_scores[metric] = z_score
        p_values[metric] = p_value
    
    return z_scores, p_values

def log_ab_test_results(new_metrics, prev_metrics, z_scores, p_values, run_id, alpha=0.05):
    for metric in new_metrics:
        mlflow.log_metric(f"new_{metric}_mean", np.mean(new_metrics[metric]))
        mlflow.log_metric(f"new_{metric}_std", np.std(new_metrics[metric]))
        mlflow.log_metric(f"new_{metric}_2.5th_percentile", np.percentile(new_metrics[metric], 2.5))
        mlflow.log_metric(f"new_{metric}_97.5th_percentile", np.percentile(new_metrics[metric], 97.5))
        
        if prev_metrics:
            p_value = p_values[metric]
            z_score = z_scores[metric]
            mlflow.log_metric(f"{metric}_z_score", z_score)
            mlflow.log_metric(f"{metric}_p_value", p_value)
            
            if p_value < alpha:
                mlflow.log_text(
                    f"{metric} has statistically significant difference (p_value={p_value}, alpha={alpha})",
                    f"a_b_test_results_for_{metric}.txt")
            else:
                mlflow.log_text(
                    f"{metric} does not have statistically significant difference (p_value={p_value}, alpha={alpha})",
                    f"a_b_test_results_for_{metric}.txt")

def ab_test(test_df, model, run_id, client, experiment_id):
    new_metrics = bootstrap_metrics(test_df, model)
    
    prev_run = client.search_runs(experiment_ids=[experiment_id], order_by=["start_time DESC"], max_results=2)
    if len(prev_run) > 1:
        prev_run_id = prev_run[1].info.run_id
        prev_metrics = {}
        for metric in new_metrics:
            prev_metrics[metric]  = (client.get_metric_history(prev_run_id, f"new_{metric}_mean")[0].value,
                                     client.get_metric_history(prev_run_id, f"new_{metric}_std")[0].value)
            
        z_scores, p_values = calculate_z_test(new_metrics, prev_metrics)
        log_ab_test_results(new_metrics, prev_metrics, z_scores, p_values, run_id)
    else:
        prev_metrics = {}
        z_scores = {}
        p_values = {}
        log_ab_test_results(new_metrics, prev_metrics, z_scores, p_values, run_id)
        logger.info("No previous run found. Just logging metrics stats.")


def main(args):
    
    TRACKING_SERVER_HOST = "158.160.38.148"
    mlflow.set_tracking_uri(f"http://{TRACKING_SERVER_HOST}:8000")
    logger.info("tracking URI: %s", {mlflow.get_tracking_uri()})

    logger.info("Creating Spark Session ...")

    conf = (
	    SparkConf().setMaster("yarn").setAppName("Fraud Detection")
	        .set("spark.executor.memory", "2g")
	        .set("spark.driver.memory", "4g")
	        .set("spark.sql.execution.arrow.pyspark.enabled", "true")
            .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .set("spark.hadoop.fs.s3a.access.key", "") \
            .set("spark.hadoop.fs.s3a.secret.key", "") \
            .set("spark.hadoop.fs.s3a.endpoint", "storage.yandexcloud.net")
	)

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    logger.info("Loading Data ...")
    df = get_dataframe(spark)

    # Prepare MLFlow experiment for logging
    client = MlflowClient()
    experiment = client.get_experiment_by_name("ab_pyspark_experiment")
    experiment_id = experiment.experiment_id
    
    # Добавьте в название вашего run имя, по которому его можно будет найти в MLFlow
    run_name = 'MyRFmodelRUN' + ' ' + str(datetime.now())

    with mlflow.start_run(run_name=run_name, experiment_id=experiment_id):
    
        df = preproc(df)
        df = scale(df)
        
        logger.info("Splitting the dataset ...")
        train_df, test_df = df.randomSplit([1 - args.val_frac, args.val_frac], seed=42)

        rf_classifier = RandomForestClassifier(featuresCol='scaled_features', labelCol='tx_fraud', numTrees=100, seed=42)
        model = rf_classifier.fit(train_df)
        
        run_id = mlflow.active_run().info.run_id

        logger.info("Performing AB test ...")
        ab_test(test_df, model, run_id, client, experiment_id)

        logger.info("Saving pipeline ...")
        mlflow.spark.save_model(model, args.output_artifact)

        # logger.info("Exporting/logging pipeline ...")
        # mlflow.spark.log_model(model, args.output_artifact)

        logger.info("Done")

    spark.stop()
    

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Model (Inference Pipeline) Training")

    parser.add_argument(
        "--val_frac",
        type=float,
        default = 0.2,
        help="Size of the validation split. Fraction of the dataset.",
    )

    parser.add_argument(
        "--output_artifact",
        default="default_run_name",
        type=str,
        help="Name for the output serialized model (Inference Artifact folder)",
        required=True,
    )
    
    sys.argv = ['train.ipynb', '--val_frac', '0.2', '--output_artifact', 'run-name']
    args = parser.parse_args(sys.argv[1:])

    # args = parser.parse_args()

    main(args)

