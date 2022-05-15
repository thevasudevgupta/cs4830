# gcloud dataproc jobs submit pyspark producer.py --cluster cs4830-project --region us-central1 --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar

!pip3 install -U -q kafka-python

import os

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1"

import json

from kafka import KafkaProducer
from tqdm.auto import tqdm

# TODO: change following for demo
REAL_TIME_DATA_PATH = "gs://big-data-cs4830/project/trainingdatanyc.csv/*.csv"
BROKER = "10.128.0.34:9092"
TOPIC = "CS4830-project"
LIMIT = 200

#################################################################

# copied from `train.py` script

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder.appName("CS4830_project")
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.11:2.4.5")
    .getOrCreate()
)

INPUT_COLUMNS = [
    "Feet From Curb",
    "Violation In Front Of Or Opposite",
    "Issuing Agency",
    "Violation County",
    "Plate Type",
    "Violation Code",
    "Registration State",
    "Issuer Squad",
]
TARGET_COLUMN = "Violation Precinct"


def read_and_prepare_data(path):
    df = spark.read.option("header", "true").csv(path)
    df = df.filter(col(TARGET_COLUMN).isNotNull())

    df = df.na.fill("NULL")
    df = df.select(INPUT_COLUMNS + [TARGET_COLUMN])

    return df


#################################################################

real_time_df = read_and_prepare_data(REAL_TIME_DATA_PATH)
# data file should be small otherwise one needs to allocate bigger cluster
real_time_df = real_time_df.limit(LIMIT)

producer = KafkaProducer(
    bootstrap_servers=[BROKER], value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

pandas_df = real_time_df.toPandas()
for index, row in tqdm(pandas_df.iterrows()):
    payload = ",".join(str(x) for x in row.to_dict().values())
    producer.send(TOPIC, value="," + payload + ",")
    producer.flush()
