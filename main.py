import google.cloud
from google.cloud import pubsub
from google.cloud import container

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

import requests

import os

apiKey = os.environ['GKE_API_KEY']

cred = credentials.ApplicationDefault()
firebase_admin.initialize_app(cred, {
  'projectId': 'biopred',
})
db = firestore.client()

client = container.ClusterManagerClient()
url = "{}/apis/batch/v1/namespaces/{}/jobs".format(
  client.get_cluster('biopred', 'us-east1-b', 'biopred-cluster').self_link,
  'default'
)

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
            "biopred-{}-job".format(message),
            "biopred-prediction-job",
            "predict",
            "gcr.io/biopred/github.com/brit228/biopred-prediction@sha256:02b32fdfe1fd6e8661a727316bf0871a3734ccd90f4dec8dcac801104f6c2584",
            message
        ),
        params={'key': apiKey}
    )


subscriber = pubsub.SubscriberClient()
sub_path = subscriber.subscription_path('biopred', 'pulljobs')
future = subscriber.subscribe(subscription_path, getRequest)
