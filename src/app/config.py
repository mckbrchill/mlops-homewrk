from pathlib import Path
import os

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType


class Config:
    """Config for application."""
    backend_host: str = "0.0.0.0"
    backend_port: int = 8000

    current_dir = Path(__file__).resolve().parent.parent
    model_path = os.path.join(current_dir, 'model', 'sparkml')
    schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("tx_datetime", TimestampType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("terminal_id", IntegerType(), True),
        StructField("tx_amount", DoubleType(), True),
        StructField("tx_time_seconds", IntegerType(), True),
        StructField("tx_time_days", IntegerType(), True),
    ])
    vector_assembler_features: list =  [
        'customer_id', 'terminal_id', 'tx_amount', 'tx_time_seconds',
        'tx_time_days'
    ]
    vector_assembler_output_col: str = 'features'

    s3_bucket: str = "otus-task-n3"
    aws_access_key_id = ""
    aws_secret_access_key = ""
    run_id: str = "73decc58d8b642b4a7cc70bb3dfaf6f9"
    master: str = "local[*]"
    app_name: str = "ModelInference"