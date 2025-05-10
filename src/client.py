import boto3
from prometheus_client import start_http_server, Gauge
from datetime import datetime, timezone
import os
import time

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_ACCESS_SECRET_KEY = os.getenv("AWS_ACCESS_SECRET_KEY")
REGION_NAME = os.getenv("REGION_NAME")
ENDPOINT_URL = os.getenv("ENDPOINT_URL")


class S3Exporter:
    def __init__(self):
        self.aws_access_key_id = AWS_ACCESS_KEY_ID
        self.aws_access_secret_key = AWS_ACCESS_SECRET_KEY
        self.region_name = REGION_NAME
        self.endpoint_url = ENDPOINT_URL

        self.bucket_size = Gauge(
            "s3_bucket_size_bytes", "Size of bucket in bytes", ["bucket_name"]
        )
        self.bucket_object_count = Gauge(
            "s3_bucket_object_count", "Number of objects in bucket", ["bucket_name"]
        )
        self.bucket_last_modified = Gauge(
            "s3_bucket_last_modified_timestamp_seconds",
            "Timestamp of last modified object in bucket",
            ["bucket_name"],
        )
        self.bucket_oldest_object = Gauge(
            "s3_bucket_oldest_object_timestamp_seconds",
            "Timestamp of oldest object in bucket",
            ["bucket_name"],
        )

        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_access_secret_key,
            region_name=self.region_name,
            endpoint_url=self.endpoint_url,
        )

    def collect_bucket_metrics(self, bucket_name):
        """Сбор базовых метрик для указанного бакета"""
        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            total_size = 0
            total_objects = 0
            last_modified = datetime.min.replace(tzinfo=None)
            oldest_object = datetime.now(timezone.utc).replace(tzinfo=None)

            for page in paginator.paginate(Bucket=bucket_name):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        total_size += obj["Size"]
                        total_objects += 1
                        obj_last_modified = obj["LastModified"].replace(tzinfo=None)
                        if obj_last_modified > last_modified:
                            last_modified = obj_last_modified
                        if obj_last_modified < oldest_object:
                            oldest_object = obj_last_modified

            self.bucket_size.labels(bucket_name=bucket_name).set(total_size)
            self.bucket_object_count.labels(bucket_name=bucket_name).set(total_objects)

            if last_modified > datetime.min.replace(tzinfo=None):
                self.bucket_last_modified.labels(bucket_name=bucket_name).set(
                    last_modified.timestamp()
                )

            if oldest_object < datetime.utcnow().replace(tzinfo=None):
                self.bucket_oldest_object.labels(bucket_name=bucket_name).set(
                    oldest_object.timestamp()
                )

        except Exception as e:
            print(f"Error collecting metrics for bucket {bucket_name}: {str(e)}")

    def run(self):
        """Запуск экспортера"""
        start_http_server(9000)
        print("S3 Exporter started on port 9000")
        print(f"S3 Endpoint: {self.endpoint_url or 'AWS Default'}")

        while True:
            try:
                response = self.s3.list_buckets()
                for bucket in response["Buckets"]:
                    bucket_name = bucket["Name"]
                    self.collect_bucket_metrics(bucket_name)

            except Exception as e:
                print(f"Error in main loop: {str(e)}")

            time.sleep(60)


if __name__ == "__main__":
    exporter = S3Exporter()

    exporter.run()
