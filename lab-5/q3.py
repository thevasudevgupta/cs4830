from pyspark.context import SparkContext
from pyspark.ml import Pipeline
from pyspark.ml.classification import (DecisionTreeClassifier,
                                       LogisticRegression, OneVsRest,
                                       RandomForestClassifier)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import MinMaxScaler, StringIndexer, VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.sql.session import SparkSession


spark = SparkSession(SparkContext())

data = spark.read.format("bigquery").option("table", "big_data_lab5.iris_table").load()
data.createOrReplaceTempView("iris_data")
# data.show()
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


def calculate_accuracy(model):
    train_pred = model.transform(train_dataset).select("prediction", "label")
    test_pred = model.transform(test_dataset).select("prediction", "label")
    print(
        "Train Accuracy:",
        MulticlassClassificationEvaluator(metricName="accuracy").evaluate(train_pred),
    )
    print(
        "Test Accuracy:",
        MulticlassClassificationEvaluator(metricName="accuracy").evaluate(test_pred),
    )


pipeline = Pipeline(stages=[indexer, assembler, scaler, LRC])
model = pipeline.fit(train_dataset)
print("Logistic regression Model with scaler")
calculate_accuracy(model)


pipeline = Pipeline(stages=[indexer, assembler, DTC])
model = pipeline.fit(train_dataset)
print("Decision Tree Classifier Model with scaler")
calculate_accuracy(model)


pipeline = Pipeline(stages=[indexer, assembler, RFC])
model = pipeline.fit(train_dataset)
print("RandomForest Model")
calculate_accuracy(model)


pipeline = Pipeline(stages=[indexer, assembler, scaler, OVRC])
model = pipeline.fit(train_dataset)
print("One-versus-Rest classifier Model with scaler")
calculate_accuracy(model)
