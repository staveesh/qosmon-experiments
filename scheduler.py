import requests
import datetime
import random
import json
import os
import dateutil.parser
import util

JOB_TYPES = ["tcp_speed_test"]

START_TIME_INTERVAL = (1, 5)
JOB_PERIOD_INTERVAL = (5, 10)
server_endpoint = "http://localhost:7800/schedule"


def schedule(exp, destination, n_targets, n_jobs):
    EXP_START_TIME = datetime.datetime.utcnow()
    END_TIME = datetime.datetime.utcnow() + datetime.timedelta(hours=1)
    TARGET_SERVERS = ["10.0.1."+str(i) for i in range(1, n_targets+1)]
    for i in range(1, n_jobs + 1):
        minutes_ahead = random.randint(START_TIME_INTERVAL[0], START_TIME_INTERVAL[1])
        start_time = EXP_START_TIME + datetime.timedelta(minutes=minutes_ahead)
        job_key = '{}_job_{}'.format(exp, i)
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
        write_request_to_file(job_key, schedule_request, destination, exp)


def write_request_to_file(job_key, schedule_request, output_dir, exp):
    if not os.path.isdir(os.path.join(output_dir, exp)):
        util.create_dir(os.path.join(output_dir, exp))
    with open(os.path.join(output_dir, exp, job_key) + ".json", "w+") as json_file:
        json_file.write(json.dumps(schedule_request))


def repeat_with_data(from_dir, output_dir, exp):
    EXP_START_TIME = datetime.datetime.utcnow()
    END_TIME = datetime.datetime.utcnow() + datetime.timedelta(hours=1)
    for data_file in os.listdir(from_dir):
        with open(os.path.join(from_dir, data_file), "r") as json_file:
            schedule_request = json.load(json_file)
            md = schedule_request['jobDescription']['measurementDescription']
            previous_start = dateutil.parser.parse(md['startTime'])
            previous_end = dateutil.parser.parse(md['endTime'])
            minutes_ahead = (previous_start - (previous_end - datetime.timedelta(hours=1)))
            start_time = EXP_START_TIME + minutes_ahead
            md['startTime'] = start_time.replace(microsecond=0).isoformat() + ".000Z"
            md['endTime'] = END_TIME.replace(microsecond=0).isoformat() + ".000Z"
            job_no = md['key'].split('_')[-1]
            md['key'] = '{}_job_{}'.format(exp, job_no)
            r = requests.post(server_endpoint, json=schedule_request)
            write_request_to_file(md['key'], schedule_request, output_dir, exp)


if __name__ == '__main__':
    schedule('test', os.getcwd(), 1, 20)
