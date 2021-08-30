import pandas as pd
import dateutil.parser
import os
import json
from math import gcd
import datetime
import matplotlib.pyplot as plt
import util

TAG = 'exp_aosd'
analysis_dir = TAG + "_analysis"
metrics_dir = TAG + '_job_metrics'

if not os.path.isdir(analysis_dir):
    util.create_dir(analysis_dir)

job_request_dict = {}
job_metrics_dict = {}

for data_file in os.listdir(metrics_dir):
    with open(os.path.join(metrics_dir, data_file), "r") as fd:
        job_metrics_dict[data_file] = json.load(fd)

for data_file in os.listdir(TAG):
    job_key = data_file.replace('.json', '')
    with open(os.path.join(TAG, data_file), "r") as fd:
        job_request_dict[data_file] = json.load(fd)


def find_missed_jobs():
    d = []
    for file in os.listdir(TAG):
        print(file)
        job_key = file.replace('.json', '')
        schedule_request = job_request_dict[file]
        start_time = schedule_request['jobDescription']['measurementDescription']['startTime']
        end_time = schedule_request['jobDescription']['measurementDescription']['endTime']
        period = schedule_request['jobDescription']['jobInterval']['jobIntervalMin']
        diff = dateutil.parser.parse(end_time) - dateutil.parser.parse(start_time)
        minutes = (diff.seconds // 60)
        n_expected_instances = minutes // period
        expected_instances = {job_key + '-' + str(i) for i in range(1, n_expected_instances + 1)}
        print(sorted(expected_instances))
        run_instances = {f.replace('.json', '') for f in os.listdir(metrics_dir) if
                         (job_key + '-') in f and 'executionTime' in job_metrics_dict[f]}
        n_run_instances = len(run_instances)
        n_missed_instances = n_expected_instances - n_run_instances
        print('{} instances missed for {}'.format(n_missed_instances, job_key))
        d.append({'key': job_key, 'n_run_instances': n_run_instances, 'n_missed_instances': n_missed_instances,
                  'missed_instances': list(expected_instances - run_instances)})
    df = pd.DataFrame(d)
    df.to_csv(analysis_dir + '/' + 'missed_jobs.csv')


def get_hyper_period():
    periods = []
    for job_file, job in job_request_dict.items():
        periods.append(job['jobDescription']['jobInterval']['jobIntervalMin'])
    lcm = 1
    for num in periods:
        lcm = lcm * num // gcd(lcm, num)
    return lcm


def find_exp_start_time():
    ids_to_search = [f.replace('.json', '') + '-1.json' for f in os.listdir(TAG)]
    min_time = datetime.datetime.utcnow()
    for id in ids_to_search:
        queuing_time = job_metrics_dict[id]['addedToQueueAt']
        if min_time > queuing_time:
            min_time = queuing_time
    return min_time


def find_n_executions_in_range(start, end, job_actual_runs):
    n = 0
    start_time = dateutil.parser.parse(start)
    end_time = dateutil.parser.parse(end)
    for job_run in job_actual_runs:
        if 'completionTime' in job_metrics_dict[job_run]:
            completion_time = dateutil.parser.parse(job_metrics_dict[job_run]['completionTime'])
            if start_time <= completion_time <= end_time:
                n += 1
    return n


def find_platform_delays():
    results = {}
    job_misses = {}
    for job_file_name, job in job_request_dict.items():
        job_key = job_file_name.replace('.json', '')
        print(job_key)
        start_time = dateutil.parser.parse(job['jobDescription']['measurementDescription']['startTime'])
        end_time = dateutil.parser.parse(job['jobDescription']['measurementDescription']['endTime'])
        period = job['jobDescription']['jobInterval']['jobIntervalMin']
        expected_completion_times = {}
        i = 1
        while start_time < end_time:
            expected_completion_times[i] = start_time
            start_time = start_time + datetime.timedelta(minutes=period)
            i += 1
        actual_completion_times = {}
        job_runs = [j for j in job_metrics_dict.keys() if (job_key + '-') in j]
        for job_run in job_runs:
            i = job_metrics_dict[job_run]['instanceNumber']
            if 'completionTime' in job_metrics_dict[job_run]:
                actual_completion_times[i] = pd.to_datetime(job_metrics_dict[job_run]['completionTime']['$date'],
                                                            unit='ms')
            else:
                actual_completion_times[i] = None
        platform_delays = {}
        n_ran = 0
        for instanceNumber, expected_completion_time in expected_completion_times.items():
            if instanceNumber in actual_completion_times and actual_completion_times[instanceNumber] is not None:
                platform_delays[instanceNumber] = actual_completion_times[instanceNumber].replace(
                    tzinfo=datetime.timezone.utc) \
                                                  - expected_completion_time.replace(tzinfo=datetime.timezone.utc)
                n_ran += 1
            else:
                print('Missing instance number {}'.format(instanceNumber))
                platform_delays[instanceNumber] = None
        results[job_key] = platform_delays
        job_misses[job_key] = {'total_expected_instances': len(expected_completion_times), 'total_instances_ran': n_ran}
    df = pd.DataFrame()
    df['job_key'] = job_misses.keys()
    df['total_expected_instances'] = [job_misses[key]['total_expected_instances'] for key in job_misses.keys()]
    df['total_instances_ran'] = [job_misses[key]['total_instances_ran'] for key in job_misses.keys()]
    df['job_number'] = df['job_key'].map(lambda key: int(key.split('_')[-1]))
    df = df.sort_values('job_number')
    df.to_csv(analysis_dir + '/' + 'job_misses.csv')
    df = df.drop('job_number', axis=1)
    graph = df.plot(kind='bar', lw=2, colormap='jet', title='Job instances')
    graph.set_xlabel('Jobs')
    graph.set_ylabel('Number of instances')
    plt.xticks(range(0, len(df)), df['job_key'])
    plt.grid()
    plt.show()
    return results


def find_average_platform_delays():
    average_delays = {}
    platform_delay_results = find_platform_delays()
    print(platform_delay_results)
    for job_key, job_platform_delays in platform_delay_results.items():
        total_delay = 0
        total_instances_ran = 0
        for instance_number, delay in job_platform_delays.items():
            if delay is None:
                continue
            total_delay += (delay.total_seconds() / 60)
            total_instances_ran += 1
        if total_instances_ran > 0:
            average_delays[job_key] = total_delay / total_instances_ran
    df = pd.DataFrame(average_delays.items(), columns=['job_key', 'average_delay_min'])
    df['job_number'] = df['job_key'].map(lambda key: int(key.split('_')[-1]))
    df = df.sort_values('job_number')
    df.to_csv(analysis_dir + '/' + 'average_platform_delay.csv')
    graph = df['average_delay_min'].plot(kind='bar', lw=2, colormap='jet', title='Average platform delay')
    graph.set_xlabel('Jobs')
    graph.set_ylabel('Average platform delay')
    plt.xticks(range(0, len(df)), df['job_key'])
    plt.grid()
    plt.show()
    return average_delays


def find_waiting_times():
    waiting_times = {}
    busy_times = {}
    for job_file_name, metrics in job_metrics_dict.items():
        metrics_key = job_file_name.replace('.json', '')
        print(metrics_key)
        added_to_queue_at = pd.to_datetime(job_metrics_dict[job_file_name]['addedToQueueAt']['$date'], unit='ms')
        if 'actualDispatchTime' not in job_metrics_dict[job_file_name]:
            continue
        actual_dispatch_time = pd.to_datetime(job_metrics_dict[job_file_name]['actualDispatchTime']['$date'], unit='ms')
        delay = actual_dispatch_time - added_to_queue_at
        delay = delay.total_seconds() / 60
        job_key = metrics_key.split('-')[0]
        if job_key not in waiting_times:
            waiting_times[job_key] = []
        waiting_times[job_key].append(delay)
        if 'nodeId' not in job_metrics_dict[job_file_name]:
            continue
        if job_metrics_dict[job_file_name]['nodeId'] not in busy_times:
            busy_times[job_metrics_dict[job_file_name]['nodeId']] = 0
        busy_times[job_metrics_dict[job_file_name]['nodeId']] += job_metrics_dict[job_file_name]['executionTime']
    average_waiting_times = {}
    print(waiting_times)
    for job_key, wts in waiting_times.items():
        job_number = int(job_key.split('_')[-1])
        average_waiting_times[job_number] = sum(wts) / len(wts)

    df1 = pd.DataFrame(average_waiting_times.items(), columns=['job_number', 'average_waiting_time'])
    df1 = df1.sort_values('job_number')
    df1.to_csv(analysis_dir + '/' + 'average_waiting_times.csv')
    df2 = pd.DataFrame(busy_times.items(), columns=['node_id', 'total_busy_time'])
    df2.to_csv(analysis_dir + '/' + 'total_busy_times.csv')


def find_esr():
    hyperperiod = get_hyper_period()
    exp_start_time = find_exp_start_time()
    end_time_str = job_request_dict[TAG + 'job_1']['jobDescription']['measurementDescription']['endTime']
    end_time = dateutil.parser.parse(end_time_str)
    delta = datetime.timedelta(minutes=hyperperiod)
    while exp_start_time + delta < end_time:
        n_jobs_executed = 0
        for job, job_request in job_request_dict.items():
            job_key = job.replace('.json', '')
            job_actual_runs = [j for j in job_metrics_dict.keys() if (job_key + '-') in j]
            n_jobs_executed += find_n_executions_in_range(exp_start_time, exp_start_time + delta, job_actual_runs)
            # n_jobs_expected += find_n_expected_in_range(exp_start_time, exp_start_time + delta)
        exp_start_time += datetime.timedelta(minutes=hyperperiod)


print(find_waiting_times())
print(find_average_platform_delays())
