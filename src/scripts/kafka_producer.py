import os
import json
import logging
import argparse
from typing import Dict, NamedTuple
from pathlib import Path

import pandas as pd
from pydantic import Field
from pydantic_settings import BaseSettings
from kafka import KafkaProducer
from kafka.errors import KafkaError

# os.environ["AWS_ENDPOINT_URL"] = "https://storage.yandexcloud.net"
# os.environ["AWS_ACCESS_KEY_ID"] = ""
# os.environ["AWS_SECRET_ACCESS_KEY"] = ""

class Config(BaseSettings):
    """
    Configuration class for Kafka settings.
    """
    local_path: Path = Path(__file__).resolve().parent.parent
    ssl_cafile: str = "../ca-certificates/Yandex/YandexInternalRootCA.crt"
    kafka_bootstrap_server: str = Field(..., env='KAFKA_BOOTSTRAP_SERVER')
    kafka_user: str = Field(..., env='KAFKA_USER')
    kafka_password: str = Field(..., env='KAFKA_PASSWORD')

    class Config:
        env_file = os.path.join(os.path.dirname(__file__), '../..', '.env')
        env_file_encoding = 'utf-8'


class RecordMetadata(NamedTuple):
    """
    A named tuple to hold metadata of a Kafka record.

    Attributes:
        topic (str): The topic to which the message was sent.
        partition (int): The partition to which the message was sent.
        offset (int): The offset of the message in the partition.
    """
    topic: str
    partition: int
    offset: int


class KafkaTransactionProducer:
    """
    Kafka producer class to send transaction messages.
    """
    def __init__(self, topic: str, num_messages: int):
        """
        Initialize KafkaTransactionProducer with topic and number of messages.

        Args:
            topic (str): Kafka topic to produce to.
            num_messages (int): Number of messages to send.
        """
        self.config = Config()
        self.topic = topic
        self.num_messages = num_messages
        self.producer = KafkaProducer(
            bootstrap_servers=self.config.kafka_bootstrap_server,
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_plain_username=self.config.kafka_user,
            sasl_plain_password=self.config.kafka_password,
            ssl_cafile=self.config.ssl_cafile,
            value_serializer=self.serialize,
        )

    @staticmethod
    def serialize(msg: Dict) -> bytes:
        """
        Serialize the message to JSON format.

        Args:
            msg (Dict): The message to serialize.

        Returns:
            bytes: Serialized message.
        """
        for key, value in msg.items():
            if isinstance(value, pd.Timestamp):
                msg[key] = value.isoformat()
        return json.dumps(msg).encode('utf-8')

    def send_message(self, transaction: Dict) -> RecordMetadata:
        """
        Send a single message to Kafka.

        Args:
            transaction (Dict): The transaction data to send.

        Returns:
            RecordMetadata: Metadata of the sent record.
        """
        future = self.producer.send(
            topic=self.topic,
            key=str(transaction['tranaction_id']).encode('ascii'),
            value=transaction
        )
        record_metadata = future.get(timeout=10)
        return RecordMetadata(
            topic=record_metadata.topic,
            partition=record_metadata.partition,
            offset=record_metadata.offset
        )

    def produce(self, transactions: pd.DataFrame) -> None:
        """
        Produce messages to Kafka from a DataFrame of transactions.

        Args:
            transactions (pd.DataFrame): DataFrame containing transaction data.
        """
        count = 0
        try:
            for i in range(min(self.num_messages, len(transactions))):
                transaction = transactions.iloc[i].to_dict()
                record_md = self.send_message(transaction)
                print(f"Msg sent. Topic: {record_md.topic}, partition: {record_md.partition}, offset: {record_md.offset}")
                print(f"Transaction: {transaction}")
                count += 1
        except KafkaError as err:
            logging.exception(err)
        finally:
            print(f"Total {count} messages sent")
            self.producer.flush()
            self.producer.close()


if __name__ == "__main__":
    argparser = argparse.ArgumentParser(description="Kafka transaction producer")
    argparser.add_argument("-t", "--topic", required=True, help="Kafka topic to produce to")
    argparser.add_argument("-n", "--num_messages", type=int, default=1000, help="Number of messages to send")
    argparser.add_argument("-f", "--file", required=True, help="Path to the parquet file with transaction data")
    args = argparser.parse_args()

    transactions = pd.read_parquet(args.file)

    producer = KafkaTransactionProducer(topic=args.topic, num_messages=args.num_messages)
    producer.produce(transactions)