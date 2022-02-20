import datetime
import json
import os

import dateutil
import matplotlib.pyplot as plt
import pandas as pd

plt.style.use('classic')

hosts = [3, 4, 5, 6, 7, 8, 9]
algos = ['random', 'rr', 'edf', 'dosd', 'aosd']

data_dir = '/home/taveesh/Desktop/QoSMon/qosmon-experiments/clique_multipath_same_jobs'

current_dir = '/home/taveesh/Desktop/QoSMon/qosmon-experiments'

dataset = {}
success_data = []
platform_data = []
wt_data = []
for n_hosts in hosts:
    dataset[n_hosts] = {}
    os.chdir(os.path.join(data_dir, 'experiment_{}'.format(n_hosts)))
    for algo in algos:
        dataset[n_hosts][algo] = {}
        jobs = {}
        for job_data in os.listdir(os.path.join('exp_' + algo)):
            job_number = int(job_data.split('_')[3].split('.')[0])
            with open(os.path.join(os.getcwd(), 'exp_' + algo, job_data), 'r') as fd:
                jobs[job_number] = json.load(fd)

        result_df = pd.read_csv('influx_results_' + algo + '.csv')
        for job, request in jobs.items():
            dataset[n_hosts][algo][job] = {}
            start_time = dateutil.parser.parse(request['jobDescription']['measurementDescription']['startTime'])
            end_time = dateutil.parser.parse(request['jobDescription']['measurementDescription']['endTime'])
            period = request['jobDescription']['jobInterval']['jobIntervalMin']
            target_server = request['jobDescription']['measurementDescription']['parameters']['target']
            job_type = request['jobDescription']['measurementDescription']['type']
            expected_instance_start = {}
            platform_delays = {}
            waiting_times = {}
            num_expected_runs = 0
            num_actual_runs = 0
            while start_time <= end_time + datetime.timedelta(minutes=period):
                num_expected_runs += 1
                expected_instance_start[num_expected_runs] = str(start_time.replace(tzinfo=datetime.timezone.utc))
                start_time = start_time + datetime.timedelta(minutes=period)
            for metrics_file in os.listdir(os.path.join(os.getcwd(), 'exp_' + algo + '_job_metrics')):
                if metrics_file.startswith('exp_' + algo + '_job_' + str(job) + '-'):
                    instance_number = int(metrics_file.split('-')[1].split('.')[0])
                    with open(os.path.join(os.getcwd(), 'exp_' + algo + '_job_metrics', metrics_file),
                              'r') as fd:
                        metrics = json.load(fd)
                        if 'dispatchTime' in metrics:
                            waiting_times[instance_number] = (dateutil.parser.parse(
                                metrics['dispatchTime']['$date']) -
                                                              dateutil.parser.parse(
                                                                  metrics['addedToQueueAt'][
                                                                      '$date'])).total_seconds() / 60
                        if 'completionTime' in metrics:
                            num_actual_runs += 1
                            platform_delays[instance_number] = (dateutil.parser.parse(
                                metrics['completionTime']['$date']) \
                                                                - dateutil.parser.parse(
                                        expected_instance_start[instance_number])).total_seconds() / 60
            job_platform_delay = 0
            for instance_number, delay in platform_delays.items():
                job_platform_delay += delay
            job_waiting_time = 0
            for instance_number, waiting_time in waiting_times.items():
                job_waiting_time += waiting_time

            average_job_platform_delay = job_platform_delay / len(platform_delays)

            average_job_waiting_time = job_waiting_time / len(waiting_times)

            success_data.append(
                (n_hosts, algo, job, job_type, period, target_server, 100 * (num_actual_runs /
                                                                             num_expected_runs)))
            platform_data.append(
                (n_hosts, algo, job, job_type, period, target_server, average_job_platform_delay))

            wt_data.append((n_hosts, algo, job, job_type, period, target_server, average_job_waiting_time))

os.chdir(current_dir)

success_df = pd.DataFrame(
    success_data,
    columns=['Number of Hosts', 'Algorithm', 'Job Number', 'Job Type', 'Job Period', 'Target Server',
             'Job Success Rate']
)

platform_df = pd.DataFrame(
    platform_data,
    columns=['Number of Hosts', 'Algorithm', 'Job Number', 'Job Type', 'Job Period', 'Target Server',
             'Average Platform Delay']
)

wt_df = pd.DataFrame(
    wt_data,
    columns=['Number of Hosts', 'Algorithm', 'Job Number', 'Job Type', 'Job Period', 'Target Server',
             'Average Waiting Time']
)

p1 = success_df[['Algorithm', 'Job Success Rate', 'Number of Hosts']] \
    .groupby(['Algorithm', 'Number of Hosts']).mean()
data = p1.unstack('Algorithm', 'Number of Hosts')['Job Success Rate']
data.to_csv('success_data.csv')
graph = data.plot(kind='bar', rot=0)
graph.tick_params(axis='x', labelsize=36)
graph.tick_params(axis='y', labelsize=36)
graph.set_ylabel('Job Success Rate (%)', fontsize=40)
graph.legend(prop={'size': 24}, loc='upper left', bbox_to_anchor=(1, 1))
plt.show()

p2 = platform_df[['Algorithm', 'Average Platform Delay', 'Number of Hosts']].groupby(
    ['Algorithm', 'Number of Hosts']).mean()
data = p2.unstack('Algorithm', 'Number of Hosts')['Average Platform Delay']
data.to_csv('platform_data.csv')
graph = data.plot(kind='bar', rot=0)
graph.tick_params(axis='x', labelsize=36)
graph.tick_params(axis='y', labelsize=36)
graph.set_ylabel('Average Platform Delay (min)', fontsize=40)
graph.legend(prop={'size': 24}, loc='upper left', bbox_to_anchor=(1, 1))
plt.show()

p3 = wt_df[['Algorithm', 'Average Waiting Time', 'Number of Hosts']].groupby(
    ['Algorithm', 'Number of Hosts']).mean()
data = p3.unstack('Algorithm', 'Number of Hosts')['Average Waiting Time']
data.to_csv('waiting_data.csv')
graph = data.plot(kind='bar', rot=0)
graph.tick_params(axis='x', labelsize=36)
graph.tick_params(axis='y', labelsize=36)
graph.set_ylabel('Average Waiting Time (min)', fontsize=40)
graph.legend(prop={'size': 24}, loc='upper left', bbox_to_anchor=(1, 1))
plt.show()
