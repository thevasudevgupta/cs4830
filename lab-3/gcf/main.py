import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def lab3_workflow(data, context):
    GCS_BUCKET_NAME = "gs://big-data-cs4830"
    TEMP_LOCATION = f"{GCS_BUCKET_NAME}/lab-3/gcf/temp"
    OUTPUT_Q1_FILE = f"{GCS_BUCKET_NAME}/lab-3/gcf/q1_output.txt"
    OUTPUT_Q2_FILE = f"{GCS_BUCKET_NAME}/lab-3/gcf/q2_output.txt"

    INPUT_DATA_FILE = f"gs://{data['bucket']}/{data['name']}"

    beam_options = PipelineOptions(
        project="gsoc-wav2vec2",
        job_name="lab-3-assignment-gcf",
        temp_location=TEMP_LOCATION,
        runner="DataflowRunner",
        region="us-central1",
    )

    def count_words(line):
        return len(re.findall(r"[\w\']+", line, re.UNICODE))

    with beam.Pipeline(options=beam_options) as p:
        lines = p | "Read data from file" >> beam.io.ReadFromText(INPUT_DATA_FILE)
        num_lines = lines | "Count number of lines" >> beam.combiners.Count.Globally()
        num_lines | "Write Q1 results to file" >> beam.io.WriteToText(OUTPUT_Q1_FILE)

        words_count_per_line = lines | "Count number of words per line" >> beam.Map(count_words)
        average_number_of_words = words_count_per_line | "Average number of words" >> beam.combiners.Mean.Globally()
        average_number_of_words | "Write Q2 results to file" >> beam.io.WriteToText(OUTPUT_Q2_FILE)
