import logging
import os
import subprocess

# List of packages to install
packages = [
    "findspark==2.0.1",
    "fsspec",
    "s3fs"
]

# Install each package
for package in packages:
    subprocess.check_call(["pip", "install", package])

import s3fs
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType, TimestampType)

cols = ['tranaction_id',
 'tx_datetime',
 'customer_id',
 'terminal_id',
 'tx_amount',
 'tx_time_seconds',
 'tx_time_days',
 'tx_fraud',
 'tx_fraud_scenario']


def list_bucket(bucket_name):
    fs = s3fs.S3FileSystem(anon=True,
                      endpoint_url="https://storage.yandexcloud.net")
    bucket_objects = fs.ls(bucket_name)
    bucket_objects = [x for x in bucket_objects if x.endswith(".txt")]
    
    return bucket_objects
                      
def replace_midnight(df):
    return df.withColumn("tx_datetime", when(df["tx_datetime"].substr(12, 8) == "24:00:00", 
                                       concat(date_add(df["tx_datetime"], 1), lit(" 00:00:00"))
                                      ).otherwise(df["tx_datetime"]))

def cast_dtypes(df):
    df = df.withColumn("tranaction_id", df["tranaction_id"].cast("int"))
    df = df.withColumn("tx_datetime", to_timestamp(df["tx_datetime"]))
    df = df.withColumn("customer_id", df["customer_id"].cast("int"))
    df = df.withColumn("terminal_id", df["terminal_id"].cast("int"))
    df = df.withColumn("tx_amount", df["tx_amount"].cast("double"))
    df = df.withColumn("tx_time_seconds", df["tx_time_seconds"].cast("int"))
    df = df.withColumn("tx_time_days", df["tx_time_days"].cast("int"))
    df = df.withColumn("tx_fraud", df["tx_fraud"].cast("int"))
    df = df.withColumn("tx_fraud_scenario", df["tx_fraud_scenario"].cast("int"))                  
    df = df.dropna()
    
    return df
                      
def drop_negative_values(df):
    for col_name in df.columns:
        if df.schema[col_name].dataType in ["int", "double"]:
            df = df.filter(col(col_name) >= 0)
    
    return df

def drop_dates(filepath, df):

    filename_date = filepath.split('/')[1].split('.')[0]              
    df = df.filter(to_date(col("tx_datetime")) == filename_date)
    return df

def save_to_parquet(output_bucket, filepath, df):
    
    fn = filepath.split("/")[1]
    output_path = f"s3a://{output_bucket}/{fn}"
    df.write.parquet(output_path)
    
    logging.info(f"File {fn} was successefully saved after cleaning")


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Data Cleaning") \
        .getOrCreate()

    
    bucket_name = "otus-task-n2"
    output_bucket = "otus-task-n3"
    file_paths = list_bucket(bucket_name)
    
    for filepath in file_paths[:1]:

        df = spark.read. \
                option("sep", ","). \
                option("comment", "#"). \
                csv(f"s3a://{filepath}", header=False).toDF(*cols)
                      
        
        df.show(10)
        df = df.dropDuplicates()
        df = drop_dates(filepath, df)              
        df = replace_midnight(df)
        df = cast_dtypes(df)
        df = drop_negative_values(df)
        df = df.orderBy("tranaction_id")
        
        save_to_parquet(output_bucket, filepath, df)
    
    spark.stop()