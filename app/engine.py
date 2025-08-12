import os
import socket
from datetime import datetime

import boto3
import pandas as pd
import requests as rq
from botocore.client import Config
from config import AppConfig, load_config
from pyspark.sql import SparkSession


class GageClinet:
    def __init__(self, conf: AppConfig):
        self.conf = conf
        self.client = self.initialize_client()

    def initialize_client(self):
        return boto3.Session(
            aws_access_key_id=self.conf.storage.access_key,
            aws_secret_access_key=self.conf.storage.secret_key,
        ).client(
            service_name="s3",
            endpoint_url=self.conf.storage.endpoint_url,
            # verify="/etc/ssl/certs/DigitalCity.pem",
            # config=Config(
            #    proxies={"http": self.conf.proxy_ip, "https": self.conf.proxy_ip}
            # ),
        )

    def find_object(
        self,
        dt_range: tuple[str, str],
        dtype: str,
        bucket_nm: str,
        topcode=None,
    ) -> list[str]:
        from_dt, to_dt = (
            datetime.strptime(dt_range[0], "%Y%m%d"),
            datetime.strptime(dt_range[1], "%Y%m%d"),
        )
        paginator = self.client.get_paginator("list_objects_v2")
        obj_lst = []
        for one_day in pd.date_range(from_dt.strftime("%Y-%m-%d"), to_dt.strftime("%Y-%m-%d")):
            # load object list
            prefix = f"MQM/primitive/{dtype}_SPC/k_year={one_day.year}/k_mon={one_day.month:02d}/k_day={one_day.day:02d}"
            obj_iterator = paginator.paginate(Bucket=bucket_nm, Prefix=prefix)
            for objs in obj_iterator:
                if objs["KeyCount"] == 0:
                    continue
                for obj in objs["Contents"]:
                    # skip empty file from file merging batch using SPARK
                    if len(obj["Key"].split("/")) != 9:  # 정상인 파일은 9개 구간으로 나뉘어져야 함
                        continue
                    # filter using date and topcode
                    obj_topcode = obj["Key"].split("/")[7].split("=")[1]
                    if (topcode is None) or (obj_topcode in topcode):
                        obj_lst.append(f"s3a://{bucket_nm}/" + obj["Key"])
        return obj_lst


class SparkSessionBuilder:
    def __init__(self, conf: AppConfig):
        self.conf = conf
        self.session = self.start_session()

    def start_session(self):
        return (
            SparkSession.builder.appName(self.conf.engine.app_name)
            .master(self.conf.engine.master)
            .config("spark.app.id", self.conf.engine.app_name)
            .config("spark.executor.instances", self.conf.engine.instance_num)
            .config("spark.executor.memory", self.conf.engine.executor_memory)
            .config("spark.executor.cores", self.conf.engine.executor_core)
            .config("spark.driver.memory", self.conf.engine.driver_memory)
            .config("spark.driver.maxResultSize", self.conf.engine.driver_max_result)
            .config(
                "spark.sql.shuffle.partitions",
                self.conf.engine.shuffle_partitions,
            )
            .config("spark.hadoop.fs.s3a.access.key", self.conf.storage.access_key)
            .config("spark.hadoop.fs.s3a.secret.key", self.conf.storage.secret_key)
            .config("spark.hadoop.fs.s3a.endpoint", self.conf.storage.endpoint_url)
            .config("spark.hadoop.com.amazonaws.services.s3.enableV4", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .config("spark.kryoserializer.buffer.max", "1024m")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config(
                "spark.kubernetes.container.image",
                "desk-docker.bart.sec.samsung.net/tmp/spark-3.2.0-base:1.2.0-20211209",
            )
            .config("spark.sql.adaptive.enabled", "true")
            .config("hive.metastore.uris", "thrift://hive-metastore-6d85cb7cdb-fvmrd")
            .config("spark.kubernetes.namespace", "spark")
            .config("spark.kubernetes.authenticate.driver.serviceAccountName", "jupyterhub")
            .config("spark.driver.port", "2222")
            .config("spark.driver.blockManager.port", "7777")
            .config("spark.driver.host", socket.gethostbyname(socket.gethostname()))
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.dynamicAllocation.enabled", "false")
            .config("spark.kubernetes.driver.deleteOnTermination", "true")
            .config("spark.kubernetes.driver.reusePersistentVolumeClaim", "true")
            .config("spark.kubernetes.driver.ownPersistentVolumeClaim", "true")
            .config(
                "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-localdirpvc.options.claimName",
                "OnDemand",
            )
            .config(
                "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-localdirpvc.options.storageClass",
                "rook-ceph-block",
            )
            .config(
                "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-localdirpvc.options.sizeLimit",
                "10Gi",
            )
            .config(
                "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-localdirpvc.mount.path",
                "/opt/spark-scratch",
            )
            .config(
                "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-localdirpvc.mount.readOnly",
                "false",
            )
            .getOrCreate()
        )

    def read_data(self, base_path: str, objects: list):
        return (
            self.session.read.format("parquet")
            .option("basePath", base_path)
            .option("mergeSchema", True)
            .load(objects)
        )
