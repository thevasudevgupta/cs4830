# gcloud dataproc clusters create cs4830-project --region us-central1 --zone us-central1-a --single-node --master-machine-type n1-standard-4 --master-boot-disk-size 500 --image-version 2.0-debian10
# gcloud dataproc jobs submit pyspark train.py --cluster cs4830-project --region us-central1 --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar


from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import (IndexToString, OneHotEncoder, StringIndexer,
                                VectorAssembler)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder.appName("CS4830_project")
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.11:2.4.5")
    .getOrCreate()
)

DATA_PATH = "gs://big-data-cs4830/project/trainingdatanyc.csv/*.csv"
MODEL_PATH = "gs://big-data-cs4830/project/testing_model"

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


data = read_and_prepare_data(DATA_PATH)
data.printSchema()

_, tr_data = data.randomSplit([0.999, 0.001], seed=42)
val_data = tr_data
print(tr_data.count())

labelIndexer = StringIndexer(inputCol=TARGET_COLUMN, outputCol="label").fit(tr_data)
feature_indexers = [
    StringIndexer(inputCol=col, outputCol=col + "_index") for col in INPUT_COLUMNS
]
feature_pipeline = Pipeline(stages=feature_indexers).fit(tr_data)
OHE = OneHotEncoder(
    inputCols=[col + "_index" for col in INPUT_COLUMNS],
    outputCols=[col + "_onehot" for col in INPUT_COLUMNS],
)
assembler = VectorAssembler(
    inputCols=[col + "_onehot" for col in INPUT_COLUMNS],
    outputCol="features",
)
model = LogisticRegression(
    featuresCol="features", labelCol="label", predictionCol="class"
)
index_to_string = IndexToString(
    inputCol="class", outputCol="prediction", labels=labelIndexer.labels
)

stages = [
    labelIndexer,
    feature_pipeline,
    OHE,
    assembler,
    model,
    index_to_string,
]
pipeline = Pipeline(stages=stages)

pipeline = pipeline.fit(tr_data)
print(f"model is trained !!")

pipeline.save(MODEL_PATH)
print(f"model saved at {MODEL_PATH} !!")

print("#" * 10, "Training Metrics", "#" * 10)

accuracy_metric = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="class", metricName="accuracy"
)
f1_metric = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="class", metricName="f1"
)
tr_pred = pipeline.transform(tr_data).select("class", "label")
tr_pred.show()

print(accuracy_metric.evaluate(tr_pred), f1_metric.evaluate(tr_pred))

print("#" * 10, "Validation Metrics", "#" * 10)

val_pred = pipeline.transform(val_data).select("class", "label")
val_pred.show()

print(accuracy_metric.evaluate(val_pred), f1_metric.evaluate(val_pred))
