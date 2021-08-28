from pymongo import MongoClient
from bson import json_util
import os
import json

client = MongoClient('mongodb://root:root@localhost:27017/')
db = client.qosmon
collection = db.job_metrics


def collect(output_dir, tag):
    dest_path = os.path.join(output_dir, tag+'_job_metrics')
    if not os.path.isdir(dest_path):
        os.mkdir(dest_path)
    for record in collection.find():
        with open(os.path.join(dest_path, record["_id"]+".json"), "w+") as json_file:
            json.dump(record, json_file, default=json_util.default)