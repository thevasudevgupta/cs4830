
import os
from google.cloud import pubsub_v1
from google.cloud import storage

project_id = "gsoc-wav2vec2"
subscription_id = "me18b182-sub"

# Create a subscriber client
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message):
    # Decode the filename back to string
    filename = message.data.decode("utf-8")
    print(f"Received file: {filename}")

    # Create a google storage client to read the uploaded file from bucket.
    storage_client = storage.Client()
    bucket_name = "cs4830-trigger-gcf"
    bucket = storage_client.get_bucket(bucket_name)

    blob = bucket.blob(filename).download_as_string().decode('utf-8')
    
    # Splits the text based on \n
    lines = blob.split('\n')
    print(f"Number of lines in {filename}: {len(lines)}")
    
    message.ack()

# Default subscriber is a pull subscriber
pull_sub_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

try:
    # The subscriber listens indefinitely 
    pull_sub_future.result()
except KeyboardInterrupt:
    pull_sub_future.cancel()
