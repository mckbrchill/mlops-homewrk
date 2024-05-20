import os

import boto3
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError
from dotenv import load_dotenv

TIMEOUT = 180
CONFIG = BotoConfig(connect_timeout=TIMEOUT, retries={"mode": "adaptive", 'max_attempts': 5},
                     tcp_keepalive=True)
load_dotenv()


def copy_s3_objects(source_bucket,
                    source_prefix,
                    destination_bucket,
                    destination_prefix,
                    endpoint_url="https://storage.yandexcloud.net"):
    s3 = boto3.client('s3',
                      aws_access_key_id=os.environ.get("S3_ID"),
                      aws_secret_access_key=os.environ.get("S3_SECRET"),
                      endpoint_url=endpoint_url,
                      config=CONFIG)

    response = s3.list_objects_v2(
        Bucket=source_bucket,
        # Prefix=source_prefix
    )

    for obj in response.get('Contents', []):
        source_key = obj['Key']

        if source_key.endswith('.txt'):
            destination_key = source_key.replace(source_prefix, destination_prefix, 1)

            copy_source = {
                'Bucket': source_bucket,
                'Key': source_key,
                # 'ACL': 'public-read'
            }

            s3.copy_object(
                CopySource=copy_source,
                Bucket=destination_bucket,
                Key=destination_key
            )
            print(f"Source {source_key} was successfully copied to {destination_bucket} with prefix {source_prefix}")

    print("Data copied successfully!")



s3_key_id = os.environ.get("S3_ID")
s3_secret = os.environ.get("S3_SECRET")
endpoint_url = os.environ.get("S3_ENDPOINT_URL")

source_bucket = os.environ.get("S3_BUCKET_NAME")
source_prefix = "fraud-data/"

destination_bucket = "otus-task-n2"
destination_prefix = source_prefix

copy_s3_objects(source_bucket,
                source_prefix,
                destination_bucket,
                destination_prefix,
                endpoint_url=endpoint_url
                )