from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType, TimestampType)


def preprocessing(file_name: str) -> None:
	conf = (
	    SparkConf().setMaster("yarn").setAppName("EDA")
	        .set("spark.executor.memory", "2g")
	        .set("spark.driver.memory", "4g")
	        .set("spark.sql.execution.arrow.pyspark.enabled", "true")
	)

	spark = SparkSession.builder.config(conf=conf).getOrCreate()
	columns = ['transaction_id',
	 'tx_datetime',
	 'customer_id',
	 'terminal_id',
	 'tx_amount',
	 'tx_time_seconds',
	 'tx_time_days',
	 'tx_fraud',
	 'tx_fraud_scenario']
	schema = StructType([
	    StructField("transaction_id", IntegerType(), True),
	    StructField("tx_datetime", TimestampType(), True),
	    StructField("customer_id", IntegerType(), True),
	    StructField("terminal_id", IntegerType(), True),
	    StructField("tx_amount", DoubleType(), True),
	    StructField("tx_time_seconds", IntegerType(), True),
	    StructField("tx_time_days", IntegerType(), True),
	    StructField("tx_fraud", IntegerType(), True),
	    StructField("tx_fraud_scenario", IntegerType(), True)
	])

	s3_filepath = f"s3a://otus-task-n2/{file_name}.txt"
	sdf = spark.read. \
	        option("sep", ","). \
	        option("comment", "#"). \
	        schema(schema). \
	        csv(s3_filepath, header=False).toDF(*columns)

	sdf = sdf.orderBy(col("tx_datetime"))
	sdf = sdf.na.drop(subset=["tx_datetime"])
	sdf = sdf.dropDuplicates(['transaction_id'])
	sdf = sdf.filter(
	    (col('transaction_id') >= 0) &
	    (col('customer_id') >= 0) &
	    (col('terminal_id') >= 0)
	)
	spark_to_s3 = SparkSession.builder \
	    .appName("Write DataFrame to S3") \
	    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
	    .config("spark.hadoop.fs.s3a.access.key", "ACCESS_KEY") \
	    .config("spark.hadoop.fs.s3a.secret.key", "SECRET_KEY") \
	    .config("spark.hadoop.fs.s3a.endpoint", "storage.yandexcloud.net") \
	    .getOrCreate()

	output_path = f"s3a://otus-task-n3/{file_name}.parquet"
	sdf.write.parquet(output_path, mode="overwrite")

	spark_to_s3.stop()
	spark.stop()


if __name__ == "__main__":
	file_name = '2019-09-21'
	preprocessing(file_name)