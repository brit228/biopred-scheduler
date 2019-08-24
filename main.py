import google.cloud
from google.cloud import pubsub
from google.cloud import container
from google.cloud import logging as CloudLogging

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

import requests

import os
import time
import logging

apiKey = os.environ['GKE_API_KEY']

cred = credentials.ApplicationDefault()
firebase_admin.initialize_app(cred, {
  'projectId': 'biopred',
})
db = firestore.client()

logging_client = CloudLogging.Client()

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
      volumes:
      - name: google-cloud-key
        secret:
          secretName: pubsub-key
      containers:
      - name: {}
        image: {}
        args: ["runPredict.py", "{}"]
        env:
        - name: GKE_API_KEY
          valueFrom:
            secretKeyRef:
              name: apisecret
              key: gkeApi
        - name: GOOGLE_APPLICATION_CREDENTIALS
          value: /var/secrets/google/key.json
        volumeMounts:
        - name: google-cloud-key
          mountPath: /var/secrets/google
      restartPolicy: Never
"""

def callback(message):
    msg = message.data.decode('utf-8')
    logging.warning(msg)
    doc_ref = db.document(msg)
    doc_ref.update({
        "status": "processing"
    })
    r = requests.post(
        url,
        headers={
            'Content-Type': 'application/yaml'
        },
        data=job_yml.format(
            "biopred-{}-job".format(msg),
            "biopred-prediction-job",
            "predict",
            "gcr.io/biopred/github.com/brit228/biopred-prediction:6b0f315",
            message
        ),
        params={'key': apiKey}
    )
    logging.warning(r.text)

subscriber = pubsub.SubscriberClient()
sub_path = subscriber.subscription_path('biopred', 'pulljobs')

while True:
    time.sleep(10)
    response = subscriber.pull(sub_path, max_messages=5)
    for msg in response.received_messages:
        callback(msg.message)
    ack_ids = [msg.ack_id for msg in response.received_messages]
    if len(ack_ids) > 0:
      subscriber.acknowledge(sub_path, ack_ids)
