# gcloud dataproc clusters create lab4 --region europe-west4 --single-node
# gcloud dataproc jobs submit pyspark q3.py --cluster lab5 --region europe-west4 --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar

from pyspark.context import SparkContext
from pyspark.ml import Pipeline
from pyspark.ml.classification import (DecisionTreeClassifier,
                                       LogisticRegression, OneVsRest,
                                       RandomForestClassifier)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import MinMaxScaler, StringIndexer, VectorAssembler
from pyspark.sql.session import SparkSession

spark = SparkSession(SparkContext())

data = spark.read.format("bigquery").option("table", "big_data_lab5.iris_table").load()
data.createOrReplaceTempView("iris_table")
query = """
SELECT *
FROM `iris_table`
WHERE sepal_length IS NOT NULL AND sepal_width IS NOT NULL AND petal_length IS NOT NULL AND petal_width IS NOT NULL AND class IS NOT NULL;
"""
data = spark.sql(query)
data.show()
train_dataset, test_dataset = data.randomSplit([0.8, 0.2])

indexer = StringIndexer(inputCol="class", outputCol="label").fit(data)
assembler = VectorAssembler(
    inputCols=["sepal_length", "sepal_width", "petal_length", "petal_width"],
    outputCol="features",
)

scaler = MinMaxScaler(inputCol="features", outputCol="features_scaled")

RFC = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=13)
LRC = LogisticRegression(maxIter=40, regParam=0.2, elasticNetParam=0.7)
OVRC = OneVsRest(classifier=LogisticRegression(maxIter=50, tol=1e-6, fitIntercept=True))
DTC = DecisionTreeClassifier(maxDepth=5, labelCol="label")


def calculate_accuracy(model, model_name):
    train_pred = model.transform(train_dataset).select("prediction", "label")
    test_pred = model.transform(test_dataset).select("prediction", "label")

    print("*" * 88)
    print(model_name)
    print(
        "Train Accuracy:",
        MulticlassClassificationEvaluator(metricName="accuracy").evaluate(train_pred),
    )
    print(
        "Test Accuracy:",
        MulticlassClassificationEvaluator(metricName="accuracy").evaluate(test_pred),
    )
    print("*" * 88)


pipeline = Pipeline(stages=[indexer, assembler, scaler, LRC])
model = pipeline.fit(train_dataset)
calculate_accuracy(model, "Logistic regression with scaler")


pipeline = Pipeline(stages=[indexer, assembler, DTC])
model = pipeline.fit(train_dataset)
calculate_accuracy(model, "Decision Tree Classifier")


pipeline = Pipeline(stages=[indexer, assembler, RFC])
model = pipeline.fit(train_dataset)
calculate_accuracy(model, "RandomForest Model")


pipeline = Pipeline(stages=[indexer, assembler, scaler, OVRC])
model = pipeline.fit(train_dataset)
calculate_accuracy(model, "One-versus-Rest Model with scaler")
