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

url = "https://kubernetes.default.svc.cluster.local/apis/batch/v1/namespaces/default/jobs"

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
        command: ["python", "runPredict.py", "{}"]
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
    doc_ref = db.document(msg)
    doc_ref.update({
        "status": "processing"
    })
    r = requests.post(
        url,
        headers={
            'Authorization': 'Bearer '+ open('/var/run/secrets/kubernetes.io/serviceaccount/token','r').read().strip(),
            'Content-Type': 'application/yaml'
        },
        data=job_yml.format(
            "biopred-{}-job".format(msg.split('/')[-1].lower()),
            "biopred-prediction-job",
            "predict",
            "gcr.io/biopred/github.com/brit228/biopred-prediction:latest",
            msg
        ),
        verify=False
    )

subscriber = pubsub.SubscriberClient()
sub_path = subscriber.subscription_path('biopred', 'pulljobs')

while True:
    time.sleep(1)
    response = subscriber.pull(sub_path, max_messages=10)
    for msg in response.received_messages:
        callback(msg.message)
    ack_ids = [msg.ack_id for msg in response.received_messages]
    if len(ack_ids) > 0:
        subscriber.acknowledge(sub_path, ack_ids)
    r = requests.get(
        url,
        headers={
            'Authorization': 'Bearer '+ open('/var/run/secrets/kubernetes.io/serviceaccount/token','r').read().strip(),
            'Content-Type': 'application/yaml'
        },
        verify=False
    )
    joblist = r.json()
    for j in joblist['items']:
        if j['status'].get('active', 0) == 0:
            if j['status'].get('failed', 0) == 0:
                r = requests.delete(
                    url+'/'+j['metadata']['name'],
                    headers={
                        'Authorization': 'Bearer '+ open('/var/run/secrets/kubernetes.io/serviceaccount/token','r').read().strip(),
                        'Content-Type': 'application/yaml'
                    },
                    data='gracePeriodSeconds: 0\norphanDependents: false\n',
                    verify=False
                )
            else:
                r = requests.delete(
                    url+'/'+j['metadata']['name'],
                    headers={
                        'Authorization': 'Bearer '+ open('/var/run/secrets/kubernetes.io/serviceaccount/token','r').read().strip(),
                        'Content-Type': 'application/yaml'
                    },
                    data='gracePeriodSeconds: 0\norphanDependents: false\n',
                    verify=False
                )
                doc_ref = db.document(j['metadata']['name'].split('-')[1])
                doc_ref.update({
                    "status": "failed"
                })
