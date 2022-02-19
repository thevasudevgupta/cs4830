gcloud functions deploy lab3_workflow \
--runtime python38 \
--trigger-resource gs://cs4830-trigger-gcf  \
--trigger-event google.storage.object.finalize

#  gcloud functions logs read --limit 50
# timout = 540
# RAM = 1 GB
# small file
