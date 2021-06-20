import json
import os

TAGS = ['exp_edf', 'exp_dosd']

results = {
    'ping': 1,
    'dns_lookup': 6,
    'traceroute': 12,
    'http': 6,
    'tcp_speed_test': 16
}

for tag in TAGS:
    for json_file in os.listdir(tag+'_job_metrics'):
        with open(tag+'_job_metrics/'+json_file, "r") as txtfile:
            content = txtfile.readline()
            metrics_dict = json.loads(content)
            if 'executionTime' in metrics_dict:
                et = metrics_dict['executionTime']
                request_file_path = tag+'/'+json_file.split('-')[0]+'.json'
                with open(request_file_path, 'r') as request_file:
                    req_content = request_file.readline()
                    request = json.loads(req_content)
                    job_type = request['jobDescription']['measurementDescription']['type']
                    results[job_type] = max(results[job_type], et)

print(results)