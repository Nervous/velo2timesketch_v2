import os
import sys
import boto3
import pytz
from datetime import datetime, timedelta
from os import path
import time
import logging
from systemd.journal import JournalHandler

log = logging.getLogger('ts_logger')
log.addHandler(JournalHandler())
log.setLevel(logging.INFO)
log.info("Started script")

session = boto3.Session(
         aws_access_key_id='$ACCESSKEY',
         aws_secret_access_key='$SECRETKEY')
s3 = session.resource('s3')
s3_client = session.client('s3')

bucket_name = 'timesketchave'
bucket = s3.Bucket(bucket_name)

while True:
    log.info("Going to sleep 60")
    time.sleep(60)
    for key in bucket.objects.all():
        time_now = datetime.utcnow().replace(tzinfo=pytz.UTC)
        delta_1s = time_now - timedelta(minutes=10)
        if key.last_modified >= delta_1s and ".zip" in key.key:

            if path.isfile("/opt/timesketch/upload/"+key.key):
                log.info("File exists, skipping")
            else:
                log.info("Files found, downloading")
                s3_client.download_file(bucket_name, key.key, '/opt/timesketch/upload/'+key.key)
