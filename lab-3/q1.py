import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

GCS_BUCKET_NAME = "gs://big-data-cs4830"
TEMP_LOCATION = f"{GCS_BUCKET_NAME}/temp-lab-3"
OUTPUT_DATA_FILE = f"{GCS_BUCKET_NAME}/lab-3/q1_output.txt"

INPUT_DATA_FILE = "gs://bdl2022/lines_big.txt"

beam_options = PipelineOptions(
    project="gsoc-wav2vec2",
    job_name="lines-count-lab-3",
    temp_location=TEMP_LOCATION,
    runner="DataflowRunner",
    region="us-central1",
    )

with beam.Pipeline(options=beam_options) as p:
    lines = p | "Reading data from file" >> beam.io.ReadFromText(INPUT_DATA_FILE)
    count = lines | "Counting number of lines in the provided file" >> beam.combiners.Count.Globally()
    count | "Writing outputs to file" >> beam.io.WriteToText(OUTPUT_DATA_FILE)
