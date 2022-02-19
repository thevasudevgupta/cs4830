import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

GCS_BUCKET_NAME = "gs://big-data-cs4830"
TEMP_LOCATION = f"{GCS_BUCKET_NAME}/temp-lab-3"
OUTPUT_DATA_FILE = f"{GCS_BUCKET_NAME}/lab-3"

INPUT_DATA_FILE = "gs://bdl2022/lines_big.txt"
# INPUT_DATA_FILE = "lines_big.txt"

beam_options = PipelineOptions(
    # project="",
    job_name="lines-count-lab-3",
    temp_location=TEMP_LOCATION,
    runner="DirectRunner",
)

# with beam.Pipeline(options=beam_options) as p:
p = beam.Pipeline(options=beam_options)
print("start")
lines = p | "Reading from file" >> beam.io.ReadFromText(INPUT_DATA_FILE)
print("1")
count = lines | beam.combiners.Count.Globally()
print("2")
count | beam.io.WriteToText(OUTPUT_DATA_FILE)
print("3")
p.run()
