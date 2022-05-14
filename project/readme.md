# Demo

```bash
gsutil cp -r gs://bdl2022/trainingdatanyc.csv gs://big-data-cs4830/project/
```

```bash
gcloud dataproc clusters create cs4830-project --enable-component-gateway --region us-central1 --zone us-central1-a --single-node --master-machine-type n1-standard-4 --master-boot-disk-size 500 --image-version 2.0-debian10 --optional-components JUPYTER --project gsoc-wav2vec2

# ssh into cluster using jupyter-lab
# create/upload notebooks for both the tasks
```

```bash
# create kafka cluster using GCP user interface
# ssh into machine and run following commands for setting up kafka
cd /opt/kafka/
sudo bin/zookeeper-server-start.sh config/zookeeper.properties
# use CTRL-Z to interrupt
bg
sudo bin/kafka-server-start.sh config/server.properties
# use CTRL-Z to interrupt
bg
sudo bin/kafka-topics.sh --create --topic CS4830-project --bootstrap-server localhost:9092
# use IP_ADDRESS of the VM created because of kafka
```

Above commands are copied from [this webpage](https://cloudinfrastructureservices.co.uk/how-to-setup-apache-kafka-server-on-azure-aws-gcp/).

Now, one just make changes at some places in `subscriber.ipynb` & `project.ipynb` and run both the files (only few snippets from later file) for the demo.
