#!/usr/bin/env python
# coding: utf-8

# https://cloudinfrastructureservices.co.uk/how-to-setup-apache-kafka-server-on-azure-aws-gcp/
# !pip3 install kafka-python

import time

from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, StringType, StructType

BROKER = "10.164.0.17:9092"
TOPIC = "me18b182-lab7"
DATA_PATH = "gs://big-data-cs4830/lab-7/iris.csv"

producer = KafkaProducer(
    bootstrap_servers=[BROKER], value_serializer=lambda row: row.encode("utf-8")
)

spark = SparkSession.builder.appName("data_producer").getOrCreate()
schema = (
    StructType()
    .add("sepal_length", FloatType())
    .add("sepal_width", FloatType())
    .add("petal_length", FloatType())
    .add("petal_width", FloatType())
    .add("class", StringType())
)

data = spark.read.csv(DATA_PATH, header=True, schema=schema)

for row in data.toJSON().collect():
    print(row)
    producer.send(TOPIC, value=row)
    time.sleep(1)
