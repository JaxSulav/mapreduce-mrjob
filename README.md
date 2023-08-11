## MapReduce Jobs using MRJob Library in GCP

This repo tries to implement some problems in a mapreduce way leveraging the MRJob library in python. You can also run this on your gcp cluster.

### Running mapreduce in local system:

` python task.py YOUR_DATA.csv > result.txt `

### Running in GCP:

`python task.py -r dataproc --cluster-id=YOUR_CLUSTER_ID --gcp-project=YOUR_PROJECT_ID YOUR_DATA.csv > result.txt`

> You can also setup a mrjob.conf file to configure some settings

```
runners:
  dataproc:
    project_id: your_project_id
    region: us-central1
    instance_type: n1-standard-1
    num_core_instances: 2
    cloud_tmp_dir: gs://bucket_dir
```
#### Make sure to update the MRJOB_CONF variable after creating

> export MRJOB_CONF=/path/to/mrjob.conf

Also, make sure to load your credentials:

> export GOOGLE_APPLICATION_CREDENTIALS="/path/to/downloaded/credentials.json"