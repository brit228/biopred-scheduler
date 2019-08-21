from google.cloud import pubsub
from google.cloud import container_v1

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

import requests

cred = credentials.ApplicationDefault()
firebase_admin.initialize_app(cred, {
  'projectId': 'biopred',
})
db = firestore.client()

client = container_v1.ClusterManagerClient()
url = client.get_cluster(name='projects/biopred/locations/us-east1-b/clusters/biopred-cluster').self_link

job_yml = """apiVersion: batch/v1
kind: Job
metadata:
  name: {}
spec:
  template:
    metadata:
      name: {}
    spec:
      containers:
      - name: {}
        image: {}
        command: ["python"]
        args: ["runPredict.py", "{}"]
      restartPolicy: Never
"""

def getRequest(message):
    doc_ref = db.document(message)
    doc_ref.update({
        "status": "processing"
    })
    requests.post(
        url,
        headers={
            'Content-Type': 'application/yaml'
        },
        data=job_yml.format(
            "{}-job".format(message),
            "prediction-job",
            "predict",
            "",
            message
        )
    )


subscriber = pubsub.SubscriberClient()
sub_path = subscriber.subscription_path('biopred', 'pulljobs')
future = subscriber.subscribe(subscription_path, callback)
