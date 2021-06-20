import requests
import datetime
import random
import json
import os
import dateutil.parser

JOB_TYPES = ["ping", "dns_lookup", "traceroute", "http", "tcp_speed_test"]
TARGET_SERVERS = ["www.google.com", "www.youtube.com", "www.tmall.com",
                  "www.baidu.com", "www.qq.com", "www.sohu.com", "www.facebook.com", "www.taobao.com"]

OUTPUT_DIR = '/home/taveesh/Desktop/QoSmon/Exp_data/randomised_90'
NUM_JOBS = 20
TAG = 'exp_rr'
EXP_START_TIME = datetime.datetime.utcnow()
START_TIME_INTERVAL = (1, 5)
END_TIME = datetime.datetime.utcnow() + datetime.timedelta(hours=2)
JOB_PERIOD_INTERVAL = (5, 10)
server_endpoint = "http://localhost:7800/schedule"

if not os.path.isdir(os.path.join(OUTPUT_DIR, TAG)):
    os.mkdir(os.path.join(OUTPUT_DIR, TAG))


def schedule():
    for i in range(1, NUM_JOBS + 1):
        minutes_ahead = random.randint(START_TIME_INTERVAL[0], START_TIME_INTERVAL[1])
        start_time = EXP_START_TIME + datetime.timedelta(minutes=minutes_ahead)
        job_key = '{}_job_{}'.format(TAG, i)
        job_type = random.choice(JOB_TYPES)
        schedule_request = {
            "jobDescription": {
                "measurementDescription": {
                    "type": job_type,
                    "key": job_key,
                    "startTime": start_time.replace(microsecond=0).isoformat() + ".000Z",
                    "endTime": END_TIME.replace(microsecond=0).isoformat() + ".000Z",
                    "count": 1,
                    "intervalSec": 1,
                    "priority": 10,
                    "parameters": {
                        "target": ('TCP_SERVER' if job_type == 'tcp_speed_test' else random.choice(TARGET_SERVERS)),
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
        # r = requests.post(server_endpoint, json=schedule_request)
        # print(r.text)
        write_request_to_file(job_key, schedule_request)


def write_request_to_file(job_key, schedule_request):
    with open(os.path.join(OUTPUT_DIR, TAG, job_key) + ".json", "w+") as json_file:
        json_file.write(json.dumps(schedule_request))


def repeat_with_data(data_dir):
    for data_file in os.listdir(data_dir):
        with open(os.path.join(data_dir, data_file), "r") as json_file:
            schedule_request = json.load(json_file)
            md = schedule_request['jobDescription']['measurementDescription']
            previous_start = dateutil.parser.parse(md['startTime'])
            previous_end = dateutil.parser.parse(md['endTime'])
            minutes_ahead = (previous_start - (previous_end - datetime.timedelta(hours=2)))
            start_time = EXP_START_TIME + minutes_ahead
            md['startTime'] = start_time.replace(microsecond=0).isoformat() + ".000Z"
            md['endTime'] = END_TIME.replace(microsecond=0).isoformat() + ".000Z"
            job_no = md['key'].split('_')[-1]
            md['key'] = '{}_job_{}'.format(TAG, job_no)
            r = requests.post(server_endpoint, json=schedule_request)
            print(r.text)
            write_request_to_file(md['key'], schedule_request)


repeat_with_data('jobs')
