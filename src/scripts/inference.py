import json
from datetime import datetime
import threading

from kafka import KafkaConsumer
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
# from pyspark.sql.streaming import GroupState
# from pyspark.sql.streaming import GroupStateTimeout
from pyspark.ml.classification import RandomForestClassificationModel
import mlflow.spark
import boto3


class Config:
    master: str = "yarn"
    app_name: str = "ModelInference"
    executor_memory: str = "2g"
    driver_memory: str = "4g"
    arrow_enabled: str = "true"
    s3_bucket: str = "otus-task-n3"
    mlflow_tracking_uri: str = "http://51.250.7.250:8000"
    mlflow_experiment: str = "ab_pyspark_experiment"
    mlflow_s3_endpoint_url: str = "https://storage.yandexcloud.net"
    aws_access_key_id = ""
    aws_secret_access_key = ""

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

    vector_assembler_features: list =  [
        'customer_id', 'terminal_id', 'tx_amount', 'tx_time_seconds',
        'tx_time_days'
    ]
    vector_assembler_output_col: str = 'features'

    group_id: str = "test"
    bootstrap_server: str = "rc1a-ff59ocdfpgsiuhmk.mdb.yandexcloud.net:9091"
    user: str = ""
    password: str = ""
    topic: str = "test"
    run_id: str = "73decc58d8b642b4a7cc70bb3dfaf6f9"
    timer: int = 600


class InferenceModels:
    def __init__(self) -> None:
        self.config = Config()
        self.spark = self.create_spark_session()
        self.model = self.load_model()
        self.output_file = self.generate_output_filename()
        self._stop_event = threading.Event()
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.config.aws_access_key_id,
            aws_secret_access_key=self.config.aws_secret_access_key,
            endpoint_url=self.config.mlflow_s3_endpoint_url
        )
        self.consumer = KafkaConsumer(
            bootstrap_servers=self.config.bootstrap_server,
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_plain_username=self.config.user,
            sasl_plain_password=self.config.password,
            ssl_cafile="YandexCA.crt",
            group_id=self.config.group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        )

    @staticmethod
    def generate_output_filename() -> str:
        """
        Generate an output filename based on the current timestamp.

        Returns:
            str: Generated filename.
        """
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"inference_{current_time}.json"

    def upload_to_s3(self) -> None:
        """
        Upload the output file to the configured S3 bucket.
        """
        try:
            self.s3_client.upload_file(self.output_file, self.config.s3_bucket, self.output_file)
            print(f"File {self.output_file} uploaded to S3 bucket {self.config.s3_bucket}")
        except Exception as e:
            print(f"Failed to upload file {self.output_file}")

    def create_spark_session(self) -> SparkSession:
        """
        Create a Spark session with specified configurations.

        Returns:
            SparkSession: Initialized Spark session.
        """
        conf = (
            SparkConf().setMaster(self.config.master).setAppName(self.config.app_name)
            .set("spark.executor.memory", self.config.executor_memory)
            .set("spark.driver.memory", self.config.driver_memory)
            .set("spark.sql.execution.arrow.pyspark.enabled", self.config.arrow_enabled)
        )

        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        return spark

    def load_model(self) -> RandomForestClassificationModel:
        """
        Load the RandomForestClassificationModel from MLflow.

        Returns:
            RandomForestClassificationModel: Loaded model.
        """
        mlflow.set_tracking_uri(self.config.mlflow_tracking_uri)
        mlflow.set_experiment(self.config.mlflow_experiment)
        model_uri = f"s3://{self.config.s3_bucket}/artifacts/1/{self.config.run_id}/artifacts/model"
        return mlflow.spark.load_model(model_uri)

    def predict(self, message: dict) -> float:
        """
        Perform prediction using the loaded model on a single message.

        Args:
            message (dict): Input message containing transaction details.

        Returns:
            float: Predicted fraud probability.
        """
        message['tx_datetime'] = datetime.strptime(message['tx_datetime'], '%Y-%m-%dT%H:%M:%S')
        sdf: DataFrame = self.spark.createDataFrame([message], schema=self.config.schema)
        assembler = VectorAssembler(inputCols=self.config.vector_assembler_features,
                                    outputCol=self.config.vector_assembler_output_col)
        sdf = assembler.transform(sdf)
        predictions = self.model.transform(sdf)
        prediction = predictions.select("prediction").collect()[0]["prediction"]
        return prediction
    
    # def stateful_predict(self, message: dict) -> float:
    #     """
    #     Perform prediction using the loaded model on a single message with saving states of some features.

    #     Args:
    #         message (dict): Input message containing transaction details.

    #     Returns:
    #         float: Predicted fraud probability.
    #     """
    #     message['tx_datetime'] = datetime.strptime(message['tx_datetime'], '%Y-%m-%dT%H:%M:%S')
    #     sdf = self.spark.createDataFrame([message], schema=self.config.schema)

    #     # Stateful preprocessing
    #     def update_customer_state(customer_id, transactions, state: GroupState):
    #         if state.exists:
    #             past_state = state.get()
    #         else:
    #             past_state = {
    #                 "total_amount": 0.0,
    #                 "count": 0,
    #                 "sum_of_squares": 0.0,
    #                 "last_tx_datetime": None
    #             }

    #         for tx in transactions.collect():
    #             tx_datetime = tx["tx_datetime"]
    #             tx_amount = tx["tx_amount"]

    #             if past_state["last_tx_datetime"] is not None:
    #                 time_since_last_tx = (tx_datetime - past_state["last_tx_datetime"]).total_seconds()
    #             else:
    #                 time_since_last_tx = None

    #             past_state["total_amount"] += tx_amount
    #             past_state["count"] += 1
    #             past_state["sum_of_squares"] += tx_amount**2
    #             past_state["last_tx_datetime"] = tx_datetime

    #             avg_tx_amount_customer = past_state["total_amount"] / past_state["count"]
    #             tx_count_customer = past_state["count"]
    #             var_tx_amount_customer = (past_state["sum_of_squares"] - (past_state["total_amount"]**2 / past_state["count"])) / past_state["count"]

    #             updated_tx = {
    #                 "tx_datetime": tx_datetime,
    #                 "customer_id": customer_id,
    #                 "tx_amount": tx_amount,
    #                 "time_since_last_tx": time_since_last_tx,
    #                 "avg_tx_amount_customer": avg_tx_amount_customer,
    #                 "tx_count_customer": tx_count_customer,
    #                 "var_tx_amount_customer": var_tx_amount_customer
    #             }

    #             yield updated_tx

    #         state.update(past_state)

    #     # Stateful preprocessing by terminal
    #     def update_terminal_state(terminal_id, transactions, state: GroupState):
    #         if state.exists:
    #             past_state = state.get()
    #         else:
    #             past_state = {
    #                 "total_amount": 0.0,
    #                 "count": 0,
    #                 "sum_of_squares": 0.0
    #             }

    #         for tx in transactions.collect():
    #             tx_datetime = tx["tx_datetime"]
    #             tx_amount = tx["tx_amount"]

    #             past_state["total_amount"] += tx_amount
    #             past_state["count"] += 1
    #             past_state["sum_of_squares"] += tx_amount**2

    #             avg_tx_amount_terminal = past_state["total_amount"] / past_state["count"]
    #             tx_count_terminal = past_state["count"]
    #             var_tx_amount_terminal = (past_state["sum_of_squares"] - (past_state["total_amount"]**2 / past_state["count"])) / past_state["count"]

    #             updated_tx = {
    #                 "tx_datetime": tx_datetime,
    #                 "terminal_id": terminal_id,
    #                 "tx_amount": tx_amount,
    #                 "avg_tx_amount_terminal": avg_tx_amount_terminal,
    #                 "tx_count_terminal": tx_count_terminal,
    #                 "var_tx_amount_terminal": var_tx_amount_terminal
    #             }

    #             yield updated_tx

    #         state.update(past_state)

    #     # Apply stateful update to the single row
    #     stateful_customer_sdf = sdf.groupBy("customer_id").flatMapGroupsWithState(
    #         outputMode="update",
    #         updateFunc=update_customer_state,
    #         stateType="Update",
    #         timeout=GroupStateTimeout.NoTimeout
    #     )

    #     stateful_terminal_sdf = sdf.groupBy("terminal_id").flatMapGroupsWithState(
    #         outputMode="update",
    #         updateFunc=update_terminal_state,
    #         stateType="Update",
    #         timeout=GroupStateTimeout.NoTimeout
    #     )

    #     # Join the stateful dataframes
    #     joined_sdf = stateful_customer_sdf.join(stateful_terminal_sdf, on=['tx_datetime', 'tx_amount'], how='inner')

    #     assembler = VectorAssembler(inputCols=self.config.numeric_columns, outputCol="scaled_features")
    #     scaler = StandardScaler(inputCol="scaled_features", outputCol="features")

    #     pipeline = Pipeline(stages=[assembler, scaler])
    #     pipeline_model = pipeline.fit(joined_sdf)
    #     sdf = pipeline_model.transform(joined_sdf)

    #     # Make predictions
    #     predictions = self.model.transform(sdf)
    #     prediction = predictions.select("prediction").collect()[0]["prediction"]
    #     return prediction

    def thread_stop_consumer(self) -> None:
        """
        Stop the Kafka consumer and upload the output file to S3 after the timer expires.
        """
        print(f"Stopping consumer after {self.config.timer} seconds")
        self._stop_event.set()
        self.upload_to_s3()
        if self.consumer:
            self.consumer.close()
        self.spark.stop()

    def upload_to_s3(self) -> None:
        try:
            self.s3_client.upload_file(self.output_file, self.config.s3_bucket, self.output_file)
            print(f"File {self.output_file} uploaded to S3 bucket {self.config.s3_bucket}")
        except Exception as e:
            print(f"Failed to upload file to S3: {e}")

    def consume_messages(self) -> None:
        """
        Consume messages from Kafka topic, perform prediction, and print results.
        """
        self.consumer.subscribe([self.config.topic])

        timer = threading.Timer(self.config.timer, self.thread_stop_consumer)
        timer.start()

        print("Waiting for new messages. Press Ctrl+C to stop")
        count = 0
        try:
            with open(self.output_file, 'a') as file:
                for msg in self.consumer:
                    if self._stop_event.is_set():
                        break
                    transaction = msg.value
                    prediction = self.predict(transaction)
                    transaction['prediction'] = prediction
                    transaction['tx_datetime'] = transaction['tx_datetime'].strftime('%Y-%m-%dT%H:%M:%S')
                    file.write(json.dumps(transaction) + '\n')
                    print(f"Transaction ID: {transaction['transaction_id']}, Prediction: {prediction}")
                    count += 1
        except KeyboardInterrupt:
            print(f"Total {count} messages received")
        finally:
            if not self._stop_event.is_set():
                self.upload_to_s3()
            self.consumer.close()
            self.spark.stop()


if __name__ == "__main__":
    predictor = InferenceModels()
    predictor.consume_messages()