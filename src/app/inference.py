from config import Config
from datetime import datetime
import pandas as pd
from typing import List

from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import PipelineModel
# from pyspark.ml.classification import RandomForestClassificationModel


class InferenceModel:
    def __init__(self) -> None:
        self.config = Config()
        self.spark = self.create_spark_session()
        self.model = self.load_model()


    def create_spark_session(self) -> SparkSession:
        """
        Create a Spark session with specified configurations.

        Returns:
            SparkSession: Initialized Spark session.
        """
        conf = (
            SparkConf().setMaster(self.config.master).setAppName(self.config.app_name)
            # .set("spark.executor.memory", self.config.executor_memory)
            # .set("spark.driver.memory", self.config.driver_memory)
            # .set("spark.sql.execution.arrow.pyspark.enabled", self.config.arrow_enabled)
        )

        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        return spark

    def load_model(self) -> PipelineModel:
    
        """
        Load the RandomForestClassificationModel from MLflow.

        Returns:
            RandomForestClassificationModel: Loaded model.
        """
        model_path = "./app/model/sparkml"
        model = PipelineModel.load(model_path)
        return model

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
    
    def predict_list(self, transactions: List[dict]) -> List[float]:
        """
        Perform prediction using the loaded model on a list os messages.

        Args:
            transactions: Input messages containing transaction details.

        Returns:
            float: Predicted fraud probability.
        """
        transactions_dict = [t.dict() for t in transactions]
        transactions_df = pd.DataFrame(transactions_dict)
        transactions_df['tx_datetime'] = pd.to_datetime(transactions_df['tx_datetime'], format='%Y-%m-%dT%H:%M:%S')

        sdf: DataFrame = self.spark.createDataFrame(transactions_df, schema=self.config.schema)
        assembler = VectorAssembler(inputCols=self.config.vector_assembler_features,
                                    outputCol=self.config.vector_assembler_output_col)
        sdf = assembler.transform(sdf)
        predictions = self.model.transform(sdf)
        prediction_list = [row["prediction"] for row in predictions.select("prediction").collect()]
        return prediction_list
    