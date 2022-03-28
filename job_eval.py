import datetime
import json
import os
import numpy as np

import dateutil
import matplotlib.pyplot as plt
import warnings

import pandas as pd
from pandas.core.common import SettingWithCopyWarning

warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)
warnings.simplefilter(action="ignore", category=FutureWarning)

plt.style.use('classic')

baseline_speed = 10000
hosts = range(3, 16)
algos = ['random', 'rr', 'edf', 'dosd', 'aosd']

data_dir = os.path.join(os.getcwd(), 'phase2_final')

current_dir = os.getcwd()

success_data = []
platform_data = []
wt_data = []
observer_data = []

algo_names = {'random': 'Random', 'rr': 'RR', 'edf': 'EDF', 'dosd': 'DOSD', 'aosd': 'AOSD'}


def timestamp(dt):
    dt = dateutil.parser.parse(dt)
    dt = dt.replace(tzinfo=None)
    epoch = datetime.datetime.utcfromtimestamp(0)
    return (dt - epoch).total_seconds() * 1000.0


for n_hosts in hosts:
    os.chdir(os.path.join(data_dir, 'experiment_{}'.format(n_hosts)))
    for algo in algos:
        jobs = {}
        for job_data in os.listdir(os.path.join('exp_' + algo)):
            job_number = int(job_data.split('_')[3].split('.')[0])
            with open(os.path.join(os.getcwd(), 'exp_' + algo, job_data), 'r') as fd:
                jobs[job_number] = json.load(fd)

        result_df = pd.read_csv('influx_results_' + algo + '.csv')
        result_df['error'] = 100 * abs(result_df['median_speed'] - baseline_speed) / baseline_speed
        for job, request in jobs.items():
            start_time = dateutil.parser.parse(request['jobDescription']['measurementDescription']['startTime'])
            end_time = dateutil.parser.parse(request['jobDescription']['measurementDescription']['endTime'])
            period = request['jobDescription']['jobInterval']['jobIntervalMin']
            target_server = request['jobDescription']['measurementDescription']['parameters']['target']
            job_type = request['jobDescription']['measurementDescription']['type']
            job_key = request['jobDescription']['measurementDescription']['key']
            expected_instance_start = {}
            platform_delays = {}
            waiting_times = {}
            num_expected_runs = 0
            while start_time <= end_time:
                num_expected_runs += 1
                expected_instance_start[num_expected_runs] = str(start_time.replace(tzinfo=datetime.timezone.utc))
                start_time = start_time + datetime.timedelta(minutes=period)
            job_df = result_df[result_df['taskKey'] == job_key]
            num_actual_runs = len(job_df)
            for idx, row in job_df.iterrows():
                job_df.at[idx, 'expected_instance_start'] = expected_instance_start[row['instance_number']]
            job_df['expected_instance_start'] = pd.to_datetime(job_df['expected_instance_start'])
            job_df['added_to_queue_at'] = pd.to_datetime(job_df['added_to_queue_at'])
            job_df['dispatch_time'] = pd.to_datetime(job_df['dispatch_time'])
            job_df['expected_instance_start'] = job_df['expected_instance_start'].astype(np.int64) / int(1e6)
            job_df['added_to_queue_at'] = job_df['added_to_queue_at'].astype(np.int64) / int(1e6)
            job_df['dispatch_time'] = job_df['dispatch_time'].astype(np.int64) / int(1e6)
            job_df['platform_delay'] = (job_df['completion_time'] - job_df['expected_instance_start']) / (1000 * 60)
            job_df['waiting_time'] = (job_df['dispatch_time'] - job_df['added_to_queue_at']) / (1000 * 60)
            average_job_platform_delay = job_df['platform_delay'].mean()
            average_job_waiting_time = job_df['waiting_time'].mean()
            success_data.append(
                (n_hosts, algo_names[algo], job, job_type, period, target_server, 100 * (num_actual_runs /
                                                                             num_expected_runs)))
            platform_data.append(
                (n_hosts, algo_names[algo], job, job_type, period, target_server, average_job_platform_delay))

            wt_data.append((n_hosts, algo_names[algo], job, job_type, period, target_server, average_job_waiting_time))
        observer_data.append((n_hosts, algo_names[algo], result_df['error'].mean()))
os.chdir(current_dir)

success_df = pd.DataFrame(
    success_data,
    columns=['Num_Hosts', 'Algorithm', 'Job Number', 'Job Type', 'Job Period', 'Target Server',
             'JSR']
)

platform_df = pd.DataFrame(
    platform_data,
    columns=['Num_Hosts', 'Algorithm', 'Job Number', 'Job Type', 'Job Period', 'Target Server',
             'APD']
)

wt_df = pd.DataFrame(
    wt_data,
    columns=['Num_Hosts', 'Algorithm', 'Job Number', 'Job Type', 'Job Period', 'Target Server',
             'AWT']
)

observer_df = pd.DataFrame(
    observer_data,
    columns=['Num_Hosts', 'Algorithm', 'Error']
)

p0 = observer_df[['Algorithm', 'Error', 'Num_Hosts']] \
    .groupby(['Algorithm', 'Num_Hosts']).mean()
data = p0.unstack('Algorithm', 'Num_Hosts')['Error']
data.to_csv('gnuplot/observer.dat', sep='\t')
graph = data.plot(kind='bar', rot=0)
graph.tick_params(axis='x', labelsize=36)
graph.tick_params(axis='y', labelsize=36)
graph.set_ylabel('Average Measurement Error (%)', fontsize=40)
graph.legend(prop={'size': 24}, loc='upper left', bbox_to_anchor=(1, 1))
plt.show()

p1 = success_df[['Algorithm', 'JSR', 'Num_Hosts']] \
    .groupby(['Algorithm', 'Num_Hosts']).mean()
data = p1.unstack('Algorithm', 'Num_Hosts')['JSR']
data.to_csv('gnuplot/success.dat', sep='\t')
graph = data.plot(kind='bar', rot=0)
graph.tick_params(axis='x', labelsize=36)
graph.tick_params(axis='y', labelsize=36)
graph.set_ylabel('Job Success Rate (%)', fontsize=40)
graph.legend(prop={'size': 24}, loc='upper left', bbox_to_anchor=(1, 1))
plt.show()

p2 = platform_df[['Algorithm', 'APD', 'Num_Hosts']].groupby(
    ['Algorithm', 'Num_Hosts']).mean()
data = p2.unstack('Algorithm', 'Num_Hosts')['APD']
data.to_csv('gnuplot/platform.dat', sep='\t')
graph = data.plot(kind='bar', rot=0)
graph.tick_params(axis='x', labelsize=36)
graph.tick_params(axis='y', labelsize=36)
graph.set_ylabel('Average Platform Delay (min)', fontsize=40)
graph.legend(prop={'size': 24}, loc='upper left', bbox_to_anchor=(1, 1))
plt.show()

p3 = wt_df[['Algorithm', 'AWT', 'Num_Hosts']].groupby(
    ['Algorithm', 'Num_Hosts']).mean()
data = p3.unstack('Algorithm', 'Num_Hosts')['AWT']
data.to_csv('gnuplot/waiting.dat', sep='\t')
graph = data.plot(kind='bar', rot=0)
graph.tick_params(axis='x', labelsize=36)
graph.tick_params(axis='y', labelsize=36)
graph.set_ylabel('Average Waiting Time (min)', fontsize=40)
graph.legend(prop={'size': 24}, loc='upper left', bbox_to_anchor=(1, 1))
plt.show()

os.chdir(current_dir)

os.chdir('gnuplot')

os.system('gnuplot observer')
os.system('gnuplot success')
os.system('gnuplot platform')
os.system('gnuplot waiting')