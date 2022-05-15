# gcloud dataproc jobs submit pyspark subscriber.py --cluster cs4830-project --region us-central1 --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar

!pip3 install -q tabulate

import os

import pandas as pd
import pyspark.sql.functions as f
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from sklearn.metrics import accuracy_score, f1_score
from tabulate import tabulate

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 pyspark-shell"

spark = SparkSession.builder.appName("CS4830_project").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# TODO: change following for demo
BROKER = "10.128.0.34:9092"
TOPIC = "CS4830-project"

MODEL_PATH = "gs://big-data-cs4830/project/final_model"

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BROKER)
    .option("subscribe", TOPIC)
    .load()
)

split_cols = f.split(df.value, ",")
df = df.withColumn("Feet From Curb", split_cols.getItem(1))
df = df.withColumn("Violation In Front Of Or Opposite", split_cols.getItem(2))
df = df.withColumn("Issuing Agency", split_cols.getItem(3))
df = df.withColumn("Violation County", split_cols.getItem(4))
df = df.withColumn("Plate Type", split_cols.getItem(5))
df = df.withColumn("Violation Code", split_cols.getItem(6))
df = df.withColumn("Registration State", split_cols.getItem(7))
df = df.withColumn("Issuer Squad", split_cols.getItem(8))
df = df.withColumn("Violation Precinct", split_cols.getItem(9))

pipeline = PipelineModel.load(MODEL_PATH)

df = pipeline.transform(df).select("Violation Precinct", "prediction")


def batch_function(df, batch_id):
    if df.count() > 0:
        print("#" * 88)
        print("Batch:", batch_id, "| COUNT:", df.count())

        dftemp = df.toPandas()
        acc = accuracy_score(dftemp["prediction"], dftemp["Violation Precinct"])
        f1 = f1_score(
            dftemp["prediction"], dftemp["Violation Precinct"], average="weighted"
        )
        output = pd.DataFrame(
            [["Accuracy", acc], ["F1-Score", f1]], columns=["Metric", "Value"]
        )

        print(tabulate(dftemp, headers="keys", tablefmt="psql", showindex=False))
        print(tabulate(output, headers="keys", tablefmt="psql", showindex=False))

        print("#" * 88)


query = df.writeStream.foreachBatch(batch_function).start()
query.awaitTermination()
