#!/usr/bin/env python
# coding: utf-8

import os
from itertools import chain

import pyspark.sql.functions as f
from pyspark import SparkContext
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, StringType, StructType

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 pyspark-shell"

sc = SparkContext()
spark = SparkSession(sc)

BROKERS = "10.164.0.17:9092"
TOPIC = "me18b182-lab7"

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BROKERS)
    .option("subscribe", TOPIC)
    .load()
)

schema = (
    StructType()
    .add("sepal_length", FloatType())
    .add("sepal_width", FloatType())
    .add("petal_length", FloatType())
    .add("petal_width", FloatType())
    .add("class", StringType())
)

df = df.select(
    f.from_json(f.decode(df.value, "utf-8"), schema=schema).alias("input")
).select("input.*")

model_path = "gs://big-data-cs4830/lab-5/lab5_model"
model = PipelineModel.load(model_path)

df = df.withColumn("true_label", df["class"])
predictions = model.transform(df)

classes_map = dict(
    zip([0.0, 1.0, 2.0], ["Iris-setosa", "Iris-versicolor", "Iris-virginica"])
)

map_op = f.create_map([f.lit(x) for x in chain(*classes_map.items())])
df = predictions.withColumn("predicted_label", map_op[f.col("prediction")])[
    ["true_label", "prediction", "predicted_label"]
]
df = df.withColumn(
    "accuracy",
    f.when(f.col("predicted_label") == f.col("true_label"), 100).otherwise(0),
)

df = df[["true_label", "predicted_label", "accuracy"]]
df.createOrReplaceTempView("output")

query = (
    df.writeStream.queryName("output").outputMode("append").format("console").start()
)
query.awaitTermination()
