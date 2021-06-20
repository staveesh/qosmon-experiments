import os
import json

data_dir = '/home/taveesh/Desktop/QoSmon/Exp_data/randomised_10/exp_aosd_job_metrics'
os.chdir(data_dir)
for json_fl in os.listdir(os.getcwd()):
    new_fl = json_fl.replace('rr','aosd')
    os.rename(json_fl, new_fl)
# for json_fl in os.listdir(data_dir):
#     with open(os.path.join(data_dir, json_fl), 'r') as fd:
#         data = json.load(fd)
#     job_key = data['jobDescription']['measurementDescription']['key']
#     job_key = job_key.replace('rr','aosd')
#     data['jobDescription']['measurementDescription']['key'] = job_key
#     with open(os.path.join(data_dir, json_fl), 'w') as fd:
#         json.dump(data, fd)

# data_dir = '/home/taveesh/Desktop/QoSmon/Exp_data/randomised_10/exp_aosd_job_metrics'
# for json_fl in os.listdir(data_dir):
#     with open(os.path.join(data_dir, json_fl), 'r') as fd:
#         data = json.load(fd)
#     job_id = data['_id'].replace('rr', 'aosd')
#     job_key = data['jobKey'].replace('rr', 'aosd')
#     data['jobKey'] = job_key
#     data['_id'] = job_id
#     with open(os.path.join(data_dir, json_fl), 'w') as fd:
#         json.dump(data, fd)
