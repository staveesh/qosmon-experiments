import json

file_path = '/home/taveesh/Desktop/exp_dosd/phone_data.txt'
job_exec_times = {"ping": [], "dns_lookup": [], "traceroute": [], "tcp_speed_test": []}
lines = []
with open(file_path, "r") as txtfile:
    lines = txtfile.readlines()
for line in lines[:-1]:
    job = json.loads(line)
    job_exec_times[job["parameters"]["type"]].append(job["executionTime"])

for mmt, et in job_exec_times.items():
    print(mmt, max(et))