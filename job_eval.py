import pandas as pd
import dateutil
import datetime
import os
import matplotlib.pyplot as plt
import json

plt.style.use('classic')

jtds = [5]
hosts = [1, 5, 10, 15, 20, 25, 30]
algos = ['random', 'rr', 'edf', 'dosd', 'aosd']

data_dir = '/home/taveesh/Desktop/Exp_data/tc_1_hour_runs_jtd_5'

dataset = {}
success_data = []
platform_data = []
wt_data = []

calculate = False

if calculate:
    for jtd in jtds:
        dataset[jtd] = {}
        host_error = []
        for n_hosts in hosts:
            dataset[jtd][n_hosts] = {}
            os.chdir(os.path.join(data_dir, 'period_gt_10_{}_{}'.format(n_hosts, jtd)))
            for algo in algos:
                dataset[jtd][n_hosts][algo] = {}
                jobs = {}
                for job_data in os.listdir(os.path.join('exp_' + algo)):
                    job_number = int(job_data.split('_')[3].split('.')[0])
                    with open(os.path.join(os.getcwd(), 'exp_' + algo, job_data), 'r') as fd:
                        jobs[job_number] = json.load(fd)

                for job, request in jobs.items():
                    print('Job : ' + str(job))
                    dataset[jtd][n_hosts][algo][job] = {}
                    start_time = dateutil.parser.parse(request['jobDescription']['measurementDescription']['startTime'])
                    end_time = dateutil.parser.parse(request['jobDescription']['measurementDescription']['endTime'])
                    period = request['jobDescription']['jobInterval']['jobIntervalMin']
                    target_server = request['jobDescription']['measurementDescription']['parameters']['target']
                    job_type = request['jobDescription']['measurementDescription']['type']
                    num_expected_runs = 0
                    num_actual_runs = 0
                    expected_instance_start = {}
                    platform_delays = {}
                    waiting_times = {}
                    while start_time <= end_time + datetime.timedelta(minutes=period):
                        num_expected_runs += 1
                        expected_instance_start[num_expected_runs] = start_time \
                                                                         .replace(
                            tzinfo=datetime.timezone.utc).timestamp() * 1000
                        start_time = start_time + datetime.timedelta(minutes=period)
                    for metrics_file in os.listdir(os.path.join(os.getcwd(), 'exp_' + algo + '_job_metrics')):
                        if metrics_file.startswith('exp_' + algo + '_job_' + str(job) + '-'):
                            instance_number = int(metrics_file.split('-')[1].split('.')[0])
                            with open(os.path.join(os.getcwd(), 'exp_' + algo + '_job_metrics', metrics_file),
                                      'r') as fd:
                                metrics = json.load(fd)
                                if 'expectedDispatchTime' in metrics:
                                    waiting_times[instance_number] = (metrics['expectedDispatchTime']['$date'] -
                                                                      metrics['addedToQueueAt']['$date']) / (1000 * 60)
                                else:
                                    if 'dispatchTime' in metrics:
                                        waiting_times[instance_number] = (metrics['dispatchTime']['$date'] -
                                                                          metrics['addedToQueueAt']['$date']) / (
                                                                                     1000 * 60)
                                if 'completionTime' in metrics:
                                    num_actual_runs += 1
                                    platform_delays[instance_number] = (metrics['completionTime']['$date'] \
                                                                        - expected_instance_start[instance_number]) / (
                                                                               1000 * 60)
                    job_platform_delay = 0
                    for instance_number, delay in platform_delays.items():
                        job_platform_delay += delay
                    job_waiting_time = 0
                    for instance_number, waiting_time in waiting_times.items():
                        job_waiting_time += waiting_time

                    if len(platform_delays) == 0:
                        continue
                    average_job_platform_delay = job_platform_delay / len(platform_delays)
                    if len(waiting_times) == 0:
                        continue

                    average_job_waiting_time = job_waiting_time / len(waiting_times)

                    success_data.append(
                        (jtd, n_hosts, algo, job, job_type, period, target_server, 100 * (num_actual_runs /
                                                                                          num_expected_runs)))
                    platform_data.append(
                        (jtd, n_hosts, algo, job, job_type, period, target_server, average_job_platform_delay))

                    wt_data.append((jtd, n_hosts, algo, job, job_type, period, target_server, average_job_waiting_time))

    os.chdir(data_dir)

    success_df = pd.DataFrame(
        success_data,
        columns=['JTD', 'Number of Hosts', 'Algorithm', 'Job Number', 'Job Type', 'Job Period', 'Target Server',
                 'Job Success Rate']
    )
    success_df.to_csv('success_df.csv')

    platform_df = pd.DataFrame(
        platform_data,
        columns=['JTD', 'Number of Hosts', 'Algorithm', 'Job Number', 'Job Type', 'Job Period', 'Target Server',
                 'Average Platform Delay']
    )
    platform_df.to_csv('platform_df.csv')

    wt_df = pd.DataFrame(
        wt_data,
        columns=['JTD', 'Number of Hosts', 'Algorithm', 'Job Number', 'Job Type', 'Job Period', 'Target Server',
                 'Average Waiting Time']
    )
    wt_df.to_csv('wt_df.csv')

else:
    os.chdir(data_dir)
    success_df = pd.read_csv('success_df.csv')
    platform_df = pd.read_csv('platform_df.csv')
    wt_df = pd.read_csv('wt_df.csv')

dfs = []
for jtd in jtds:
    for n_hosts in hosts:
        for algo in algos:
            os.chdir(os.path.join(data_dir, 'period_gt_10_{}_{}'.format(n_hosts, jtd)))
            df = pd.read_csv('influx_results_{}.csv'.format(algo))
            df['JTD'] = jtd
            df['N_hosts'] = n_hosts
            df['Algorithm'] = algo
            dfs.append(df)

error_df = pd.concat(dfs)

p1 = success_df[['Algorithm', 'Job Success Rate', 'Number of Hosts']] \
    .groupby(['Algorithm', 'Number of Hosts']).mean()
graph = p1.unstack('Algorithm', 'Number of Hosts')['Job Success Rate'].plot(style='.-', rot=0)
graph.tick_params(axis='x', labelsize=36)
graph.tick_params(axis='y', labelsize=36)
graph.set_ylabel('Job Success Rate (%)', fontsize=40)
graph.legend(prop={'size': 24}, loc='upper left', bbox_to_anchor=(1, 1))
plt.show()

p2 = platform_df[['Algorithm', 'Average Platform Delay', 'Number of Hosts']].groupby(
    ['Algorithm', 'Number of Hosts']).mean()
graph = p2.unstack('Algorithm', 'Number of Hosts')['Average Platform Delay'].plot(style='.-', rot=0)
graph.tick_params(axis='x', labelsize=36)
graph.tick_params(axis='y', labelsize=36)
graph.set_ylabel('Average Platform Delay (min)', fontsize=40)
graph.legend(prop={'size': 24}, loc='upper left', bbox_to_anchor=(1, 1))
plt.show()

p3 = wt_df[['Algorithm', 'Average Waiting Time', 'Number of Hosts']].groupby(
    ['Algorithm', 'Number of Hosts']).mean()
graph = p3.unstack('Algorithm', 'Number of Hosts')['Average Waiting Time'].plot(style='.-', rot=0)
graph.tick_params(axis='x', labelsize=36)
graph.tick_params(axis='y', labelsize=36)
graph.set_ylabel('Average Waiting Time (min)', fontsize=40)
graph.legend(prop={'size': 24}, loc='upper left', bbox_to_anchor=(1, 1))
plt.show()

print(error_df.columns)

err = error_df[['Algorithm', 'max_speed', 'N_hosts']] \
    .groupby(['Algorithm', 'N_hosts']).mean()
graph = err.unstack('Algorithm', 'JTD')['max_speed'].plot(style='o-', rot=0)
graph.tick_params(axis='x', labelsize=36)
graph.tick_params(axis='y', labelsize=36)
graph.set_ylabel('Max Speed', fontsize=40)
graph.legend(prop={'size': 24}, loc='upper left', bbox_to_anchor=(1, 1))
# plt.axhline(y=10000, color='r', linestyle='-')
plt.show()