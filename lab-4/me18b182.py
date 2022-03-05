import pyspark
import sys

# gcloud dataproc clusters create lab4 --region europe-west4 --single-node
# gcloud dataproc jobs submit pyspark me18b182.py --cluster lab4 --region europe-west4 -- gs://bdl2022/lab4_dataset.csv gs://big-data-cs4830/lab-4

def make_bins(x):
  if x >= 0 and x < 6:
    return ("0-6", 1)
  elif x >= 6 and x < 12:
    return ("6-12", 1) 
  elif x >= 12 and x < 18:
    return ("12-18", 1)
  else:
    return ("18-24", 1)

def main(input_path, output_path):
    spark = pyspark.SparkContext.getOrCreate()
    data = spark.textFile(input_path)

    # remove heading from the data
    header = data.first() 
    data = data.filter(lambda row: row != header)

    # groupby & count each group
    data = data.map(lambda text: int(text.split()[1].split(':')[0]))\
            .map(make_bins)\
            .reduceByKey(lambda x, y: x + y)
    data.coalesce(1).saveAsTextFile(output_path) 


if __name__ == '__main__':
    input_path, output_path = sys.argv[1: 3]
    main(input_path, output_path)
