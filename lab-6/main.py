# gcloud pubsub topics create me18b182-pub
# gcloud pubsub subscriptions create me18b182-sub --topic me18b182-pub

# gcloud functions deploy TRIGGER_PUB \
#     --runtime python37 \
#     --trigger-resource cs4830-trigger-gcf \
#     --trigger-event google.storage.object.finalize

# python3 sub.py

# wget https://filesamples.com/samples/document/txt/sample1.txt
# gsutil cp sample1.txt gs://cs4830-trigger-gcf

# gcloud functions logs read --limit 3

def TRIGGER_PUB(data, context):
    from google.cloud import pubsub_v1

    project_id = "gsoc-wav2vec2"
    topic_id = "me18b182-pub"

    # Create a publisher client to topic
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    # Publish path to file in the topic & Data must be a bytestring
    pub_data = data['name'].encode("utf-8")

    # Client returns future when publisher publish.
    future = publisher.publish(topic_path, pub_data)

    print(f"Published {pub_data} to {topic_path}")
