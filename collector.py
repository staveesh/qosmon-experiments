from pymongo import MongoClient
from bson import json_util
import os
import json

client = MongoClient('mongodb://root:root@localhost:27017/')
db = client.qosmon
collection = db.job_metrics

TAG = "exp_rr_job_metrics"
if not os.path.isdir(TAG):
    os.mkdir(TAG)
os.chdir(TAG)
for record in collection.find():
    with open(record["_id"]+".json", "w+") as json_file:
        json.dump(record, json_file, default=json_util.default)