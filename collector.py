from pymongo import MongoClient
from bson import json_util
import os
import json

client = MongoClient('mongodb://root:root@localhost:27017/')
db = client.qosmon
collection = db.job_metrics

ids = []
OUTPUT_DIR = '/home/taveesh/Desktop/QoSmon/Exp_data/randomised_90'

TAG = "exp_rr_job_metrics"
if not os.path.isdir(os.path.join(OUTPUT_DIR, TAG)):
    os.mkdir(os.path.join(OUTPUT_DIR, TAG))
os.chdir(os.path.join(OUTPUT_DIR, TAG))
for record in collection.find():
    with open(record["_id"]+".json", "w+") as json_file:
        json.dump(record, json_file, default=json_util.default)