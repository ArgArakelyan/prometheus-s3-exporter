"""
S3 Buckets information scraper to prometheus metrics

:author - github.com/ArgArakelyan
"""

import logging
import os
import time
from datetime import datetime, timezone

import boto3
import yaml
from prometheus_client import Gauge, start_http_server

CONFIG_FILE = os.getenv("CONFIG_FILE", "config.yaml")


class S3BucketConfig:
    def __init__(
        self, name, aws_access_key_id, aws_secret_access_key, region_name=None, endpoint_url=None
    ):
        self.name = name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.endpoint_url = endpoint_url


class S3Exporter:
    def __init__(self):
        """Инициализация метрик Prometheus"""
        self.bucket_size = Gauge(
            "s3_bucket_size_bytes", "Size of bucket in bytes", ["bucket_name", "bucket_config"]
        )
        self.bucket_object_count = Gauge(
            "s3_bucket_object_count",
            "Number of objects in bucket",
            ["bucket_name", "bucket_config"],
        )
        self.bucket_last_modified = Gauge(
            "s3_bucket_last_modified_timestamp_seconds",
            "Timestamp of last modified object in bucket",
            ["bucket_name", "bucket_config"],
        )
        self.bucket_oldest_object = Gauge(
            "s3_bucket_oldest_object_timestamp_seconds",
            "Timestamp of oldest object in bucket",
            ["bucket_name", "bucket_config"],
        )

        # Загрузка конфигурации
        self.bucket_configs = self.load_config()

    def load_config(self):
        """Загрузка конфигурации из YAML файла"""
        try:
            with open(CONFIG_FILE, "r") as f:
                config = yaml.safe_load(f)

            bucket_configs = []
            for bucket_config in config.get("buckets", []):
                bucket_configs.append(
                    S3BucketConfig(
                        name=bucket_config["name"],
                        aws_access_key_id=bucket_config["aws_access_key_id"],
                        aws_secret_access_key=bucket_config["aws_secret_access_key"],
                        region_name=bucket_config.get("region_name"),
                        endpoint_url=bucket_config.get("endpoint_url"),
                    )
                )
            return bucket_configs
        except Exception as e:
            logging.error(f"Error loading config: {str(e)}")
            return []

    def create_s3_client(self, bucket_config):
        """Создание клиента S3 для конкретной конфигурации бакета"""
        return boto3.client(
            "s3",
            aws_access_key_id=bucket_config.aws_access_key_id,
            aws_secret_access_key=bucket_config.aws_secret_access_key,
            region_name=bucket_config.region_name,
            endpoint_url=bucket_config.endpoint_url,
        )

    def collect_bucket_metrics(self, bucket_config):
        """Сбор метрик для указанного бакета"""
        try:
            s3 = self.create_s3_client(bucket_config)
            paginator = s3.get_paginator("list_objects_v2")

            total_size = 0
            total_objects = 0
            last_modified = datetime.min.replace(tzinfo=None)
            oldest_object = datetime.now(timezone.utc).replace(tzinfo=None)

            for page in paginator.paginate(Bucket=bucket_config.name):
                if "Contents" in page:
                    for obj in page["Contents"]:
                        total_size += obj["Size"]
                        total_objects += 1
                        obj_last_modified = obj["LastModified"].replace(tzinfo=None)
                        if obj_last_modified > last_modified:
                            last_modified = obj_last_modified
                        if obj_last_modified < oldest_object:
                            oldest_object = obj_last_modified

            self.bucket_size.labels(
                bucket_name=bucket_config.name, bucket_config=bucket_config.aws_access_key_id[-4:]
            ).set(total_size)

            self.bucket_object_count.labels(
                bucket_name=bucket_config.name, bucket_config=bucket_config.aws_access_key_id[-4:]
            ).set(total_objects)

            if last_modified > datetime.min.replace(tzinfo=None):
                self.bucket_last_modified.labels(
                    bucket_name=bucket_config.name,
                    bucket_config=bucket_config.aws_access_key_id[-4:],
                ).set(last_modified.timestamp())

            if oldest_object < datetime.utcnow().replace(tzinfo=None):
                self.bucket_oldest_object.labels(
                    bucket_name=bucket_config.name,
                    bucket_config=bucket_config.aws_access_key_id[-4:],
                ).set(oldest_object.timestamp())

        except Exception as e:
            logging.error(f"Error collecting metrics for bucket {bucket_config.name}: {str(e)}")

    def run(self):
        """Запуск экспортера"""
        start_http_server(9000)
        logging.info("S3 Exporter started on port 9000")

        while True:
            try:
                self.bucket_configs = self.load_config()

                for bucket_config in self.bucket_configs:
                    logging.info(f"Processing bucket: {bucket_config.name}")
                    self.collect_bucket_metrics(bucket_config)

            except Exception as e:
                logging.error(f"Error in main loop: {str(e)}")

            time.sleep(60)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    exporter = S3Exporter()
    exporter.run()
