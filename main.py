import time
import logging
import requests
import datetime
import random
import json
import os
import dateutil.parser
import schedule
from pymongo import MongoClient
from bson import json_util
import shutil
import subprocess

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

TOPOLOGIES = ['10', '50', '90']
ALGORITHMS = ['rr', 'edf', 'dosd', 'aosd']
RUNS = []
RUN_INDEX = 0
for topology in TOPOLOGIES:
    for algorithm in ALGORITHMS:
        RUNS.append((topology, algorithm))
JOB_TYPES = ["ping", "dns_lookup", "traceroute", "http", "tcp_speed_test"]
TARGET_SERVERS = ["www.google.com", "www.youtube.com", "www.tmall.com",
                  "www.baidu.com", "www.qq.com", "www.sohu.com", "www.facebook.com", "www.taobao.com"]
NUM_JOBS = 20
START_TIME_INTERVAL = (1, 5)
JOB_PERIOD_INTERVAL = (5, 10)
APP_PATH = '/home/taveesh/Desktop/QoSMon/request-handler'
OUTPUT_DATA_PATH = '/home/taveesh/Desktop/Exp_data_automated/first'
DB_DATA_PATH = '/var/lib/qosmon'
server_endpoint = "http://localhost:7800/schedule"


def schedule_new(algorithm, exp_start_time, exp_end_time, output_data_dir):
    for i in range(1, NUM_JOBS + 1):
        minutes_ahead = random.randint(START_TIME_INTERVAL[0], START_TIME_INTERVAL[1])
        start_time = exp_start_time + datetime.timedelta(minutes=minutes_ahead)
        job_key = '{}_job_{}'.format('exp_' + algorithm, i)
        schedule_request = {
            "jobDescription": {
                "measurementDescription": {
                    "type": random.choice(JOB_TYPES),
                    "key": job_key,
                    "startTime": start_time.replace(microsecond=0).isoformat() + ".000Z",
                    "endTime": exp_end_time.replace(microsecond=0).isoformat() + ".000Z",
                    "count": 1,
                    "intervalSec": 1,
                    "priority": 10,
                    "parameters": {
                        "target": random.choice(TARGET_SERVERS),
                        "server": "null",
                        "dirUp": random.choice([True, False]),
                    },
                    "instanceNumber": 1
                },
                "nodeCount": 1,
                "jobInterval": {
                    "jobIntervalHr": 0,
                    "jobIntervalMin": random.randint(JOB_PERIOD_INTERVAL[0], JOB_PERIOD_INTERVAL[1]),
                    "jobIntervalSec": 0,
                },
            },
            "requestType": "SCHEDULE_MEASUREMENT",
            "userId": "taveesh08@gmail.com",
        }
        r = requests.post(server_endpoint, json=schedule_request)
        logging.info(r.text)
        write_request_to_file(job_key, schedule_request, output_data_dir)


def write_request_to_file(job_key, schedule_request, data_dir):
    with open(os.path.join(data_dir, job_key) + ".json", "w+") as json_file:
        json_file.write(json.dumps(schedule_request))


def repeat_with_data(original_data_dir, output_data_dir, algorithm, exp_start_time, exp_end_time):
    for data_file in os.listdir(original_data_dir):
        with open(os.path.join(original_data_dir, data_file), "r") as json_file:
            schedule_request = json.load(json_file)
            md = schedule_request['jobDescription']['measurementDescription']
            previous_start = dateutil.parser.parse(md['startTime'])
            previous_end = dateutil.parser.parse(md['endTime'])
            minutes_ahead = (previous_start - (previous_end - datetime.timedelta(hours=2)))
            start_time = exp_start_time + minutes_ahead
            md['startTime'] = start_time.replace(microsecond=0).isoformat() + ".000Z"
            md['endTime'] = exp_end_time.replace(microsecond=0).isoformat() + ".000Z"
            job_no = md['key'].split('_')[-1]
            md['key'] = '{}_job_{}'.format('exp_' + algorithm, job_no)
            r = requests.post(server_endpoint, json=schedule_request)
            logging.info(r.text)
            write_request_to_file(md['key'], schedule_request, output_data_dir)


def write_to_env_file(env_dict):
    with open(".env", "w") as fd:
        for k, v in env_dict.items():
            fd.write(k + "=" + str(v) + "\n")


def work():
    global RUN_INDEX
    if RUN_INDEX < len(RUNS):
        logging.info(RUNS[RUN_INDEX])
        os.chdir(APP_PATH)
        if os.path.isdir(os.path.join(DB_DATA_PATH, 'mongodb')):
            shutil.rmtree(os.path.join(DB_DATA_PATH, 'mongodb'))
        if os.path.isdir(os.path.join(DB_DATA_PATH, 'influxdb')):
            shutil.rmtree(os.path.join(DB_DATA_PATH, 'influxdb'))
        if RUN_INDEX > 0:
            client = MongoClient('mongodb://root:root@localhost:27017/')
            db = client.qosmon
            collection = db.job_metrics
            prev_topology = RUNS[RUN_INDEX - 1][0]
            prev_algo = RUNS[RUN_INDEX - 1][1]
            TAG = "exp_" + prev_algo + "_job_metrics"
            if not os.path.isdir(os.path.join(OUTPUT_DATA_PATH, prev_topology, TAG)):
                os.mkdir(os.path.join(OUTPUT_DATA_PATH, prev_topology, TAG))
            for record in collection.find():
                with open(os.path.join(OUTPUT_DATA_PATH, prev_topology, TAG, record["_id"] + ".json"), "w+") \
                        as json_file:
                    json.dump(record, json_file, default=json_util.default)
            time.sleep(5)
            subprocess.run(['docker-compose', 'down'])
            time.sleep(20)
            os.chdir('/home/taveesh')
            subprocess.run(['bash', 'android_uninstall.sh'])
            time.sleep(30)
        os.chdir(APP_PATH)
        env_vars = {
            'INFLUXDB_USERNAME': 'root',
            'INFLUXDB_PASSWORD': 'root',
            'INFLUXDB_NAME': 'mobiperf',
            'INFLUXDB_PORT': 8086,
            'MONGODB_USERNAME': 'root',
            'MONGODB_PASSWORD': 'root',
            'MONGODB_NAME': 'qosmon',
            'MONGODB_PORT': 27017,
            'MONGODB_HOST': 'mongodb',
            'HTTP_SERVER_PORT': 7800,
            'TCP_SERVER_PORT': 7000,
            'FILE_SERVER_HOSTNAME': '',
            'MILLISECONDS_TILL_RETRY_CONNECT': 120000,
            'MILLISECONDS_TILL_ANALYZE_PCAPFILES': 86400000,
            'MILLISECONDS_INIT_DELAY': 60000,
            'FILE_MONITOR_DELAY': 5000,
            'NUM_RETRY_CONNECT': 3,
            'SCHEDULING_ALGO_NAME': RUNS[RUN_INDEX][1],
            'NETWORK_TOPOLOGY_FILE': 'topology_' + RUNS[RUN_INDEX][0] + '.dot',
            'NETWORK_TOPOLOGY_EDGE_PROBABILITY': int(RUNS[RUN_INDEX][0]) / 100,
            'NETWORK_TOPOLOGY_NUM_MEASUREMENT_NODES': 4,
            'NETWORK_TOPOLOGY_NUM_ACCESS_POINTS': 8
        }
        write_to_env_file(env_vars)
        subprocess.run(['docker-compose', 'up', '--detach', '--build'])
        os.chdir('/home/taveesh')
        subprocess.run(['bash', 'android_install.sh'])
        time.sleep(30)
        logging.info('Scheduling jobs.....')
        os.chdir('/home/taveesh/Desktop/QoSMon/qosmon-experiments')
        exp_start_time = datetime.datetime.utcnow()
        exp_end_time = datetime.datetime.utcnow() + datetime.timedelta(hours=2)
        if not os.path.isdir(os.path.join(OUTPUT_DATA_PATH, RUNS[RUN_INDEX][0])):
            os.mkdir(os.path.join(OUTPUT_DATA_PATH, RUNS[RUN_INDEX][0]))
        if not os.path.isdir(os.path.join(OUTPUT_DATA_PATH, RUNS[RUN_INDEX][0], 'exp_' + RUNS[RUN_INDEX][1])):
            os.mkdir(os.path.join(OUTPUT_DATA_PATH, RUNS[RUN_INDEX][0], 'exp_' + RUNS[RUN_INDEX][1]))
        repeat_with_data('jobs', os.path.join(OUTPUT_DATA_PATH, RUNS[RUN_INDEX][0], 'exp_' + RUNS[RUN_INDEX][1]),
                         RUNS[RUN_INDEX][1],
                         exp_start_time, exp_end_time)
        RUN_INDEX += 1


schedule.every(2).hours.at("10:00").do(work)
work()
while True:
    schedule.run_pending()
    time.sleep(1)
    if RUN_INDEX >= len(RUNS):
        break
