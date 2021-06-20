import datetime
import json
import os
import random
import dateutil.parser
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

font = {'family': 'normal',
        'weight': 'bold',
        'size': 44}

matplotlib.rc('font', **font)

plt.style.use('classic')
job_specs_dir = '/home/taveesh/Desktop/QoSmon/Exp_data/job_specs'
csv_dir = '/home/taveesh/Desktop/QoSmon/Exp_data/csv'
algorithms = ['rr', 'edf', 'dosd', 'aosd']
topologies = ['10', '50', '90']

# job_dataset = []
# for jfile in os.listdir(job_specs_dir):
#     with open(os.path.join(job_specs_dir, jfile)) as jfd:
#         specs = json.load(jfd)
#         job_no = int(specs['jobDescription']['measurementDescription']['key'].split('_')[-1])
#         target_server = specs['jobDescription']['measurementDescription']['parameters']['target']
#         period = specs['jobDescription']['jobInterval']['jobIntervalMin']
#         job_type = specs['jobDescription']['measurementDescription']['type']
#         job_dataset.append({
#             'Job Number': job_no,
#             'Job Period': period,
#             'Target Server': target_server,
#             'Job Type': job_type
#         })
#
# job_df = pd.DataFrame(job_dataset)
# g = job_df[['Job Type', 'Job Number']].groupby(['Job Type']).agg(['count']).plot(kind='bar', rot=0, legend=False)
# g.tick_params(axis='x', labelsize=18)
# g.tick_params(axis='y', labelsize=18)
# g.set_xlabel('Job Type', fontsize=22)
# g.set_ylabel('Number of jobs', fontsize=22)
# plt.show()
# g = job_df[['Target Server', 'Job Number']].groupby(['Target Server']).agg(['count']).plot(kind='bar', rot=0,legend=False)
# g.tick_params(axis='x', labelsize=18)
# g.tick_params(axis='y', labelsize=18)
# g.set_xlabel('Targer Server', fontsize=22)
# g.set_ylabel('Number of jobs', fontsize=22)
# plt.show()
# g = job_df[['Job Period', 'Job Number']].groupby(['Job Period']).agg(['count']).plot(kind='bar', rot=0, legend=False)
# g.tick_params(axis='x', labelsize=18)
# g.tick_params(axis='y', labelsize=18)
# g.set_ylabel('Number of jobs', fontsize=22)
# g.set_xlabel('Job Period (min)', fontsize=22)
# plt.show()


dataset = {}
nbtr = {}
costs = {}
for topology in topologies:
    data_dir = '/home/taveesh/Desktop/QoSmon/Exp_data/data_' + topology
    dataset[topology] = {}
    nbtr[topology] = {}
    with open(os.path.join(data_dir, 'costs.json'), 'r') as cfd:
        cost_map = json.load(cfd)
    total_cost = sum(v for v in cost_map.values())
    cost_map = {k: 100 * v / total_cost for (k, v) in cost_map.items()}
    print(cost_map)
    costs[topology] = cost_map
    for algorithm in algorithms:
        with open(os.path.join(data_dir, 'nodes_' + algorithm + '.json'), 'r') as jfd:
            nodes_map = json.load(jfd)
        nodes_map = dict((v, k) for k, v in nodes_map.items())
        dataset[topology][algorithm] = {}
        nbtr[topology][algorithm] = {}
        jobs = {}
        for job_data in os.listdir(os.path.join(data_dir, 'exp_' + algorithm)):
            job_number = int(job_data.split('_')[3].split('.')[0])
            with open(os.path.join(data_dir, 'exp_' + algorithm, job_data), 'r') as fd:
                jobs[job_number] = json.load(fd)

        # Time-based metrics
        for job, request in jobs.items():
            print('Job : ' + str(job))
            dataset[topology][algorithm][job] = {}
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
            while start_time <= end_time:
                num_expected_runs += 1
                expected_instance_start[num_expected_runs] = start_time \
                                                                 .replace(
                    tzinfo=datetime.timezone.utc).timestamp() * 1000
                start_time = start_time + datetime.timedelta(minutes=period)
            for metrics_file in os.listdir(os.path.join(data_dir, 'exp_' + algorithm + '_job_metrics')):
                if metrics_file.startswith('exp_' + algorithm + '_job_' + str(job) + '-'):
                    instance_number = int(metrics_file.split('-')[1].split('.')[0])
                    with open(os.path.join(data_dir, 'exp_' + algorithm + '_job_metrics', metrics_file), 'r') as fd:
                        metrics = json.load(fd)
                        if 'expectedDispatchTime' in metrics:
                            waiting_times[instance_number] = (metrics['expectedDispatchTime']['$date'] -
                                                              metrics['addedToQueueAt']['$date']) / (1000 * 60)
                        else:
                            waiting_times[instance_number] = (metrics['dispatchTime']['$date'] -
                                                              metrics['addedToQueueAt']['$date']) / (1000 * 60)
                        if 'completionTime' in metrics:
                            if nodes_map[metrics['nodeId']] not in nbtr[topology][algorithm]:
                                nbtr[topology][algorithm][nodes_map[metrics['nodeId']]] = 0
                            nbtr[topology][algorithm][nodes_map[metrics['nodeId']]] += metrics['executionTime']
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
            average_job_platform_delay = job_platform_delay / len(platform_delays)
            average_job_waiting_time = job_waiting_time / len(waiting_times)

            dataset[topology][algorithm][job]['Algorithm'] = algorithm
            dataset[topology][algorithm][job]['Job Number'] = job
            dataset[topology][algorithm][job]['Job Type'] = job_type
            dataset[topology][algorithm][job]['Job Period'] = period
            dataset[topology][algorithm][job]['Target Server'] = target_server
            dataset[topology][algorithm][job]['Conflict Probability'] = int(topology) / 100
            dataset[topology][algorithm][job]['Job Success Rate'] = 100 * (num_actual_runs / num_expected_runs)
            dataset[topology][algorithm][job]['Average Platform Delay'] = average_job_platform_delay
            dataset[topology][algorithm][job]['Average Waiting Time'] = average_job_waiting_time

for topology in topologies:
    for algorithm in algorithms:
        total_time = sum(v for v in nbtr[topology][algorithm].values())
        nbtr[topology][algorithm] = {k: 100 * v / total_time for (k, v) in nbtr[topology][algorithm].items()}
        dict_values = sorted(list(nbtr[topology][algorithm].values()))
        temp_dict = {}
        for i in range(1, len(dict_values) + 1):
            temp_dict['M' + str(i)] = dict_values[i - 1]
        nbtr[topology][algorithm] = temp_dict

nbtr_data = []
for topology in topologies:
    scheduling_cost = costs[topology]
    for algorithm in algorithms:
        x = {
            'Conflict Probability': int(topology) / 100,
            'Algorithm': algorithm.upper(),
        }
        x.update(nbtr[topology][algorithm])
        nbtr_data.append(x)
nbtr_df = pd.DataFrame(nbtr_data)

cost_data = []
for topology in topologies:
    prob = int(topology) / 100
    x = {'Conflict Probability': prob}
    x.update(costs[topology])
    cost_data.append(x)
costs_df = pd.DataFrame(cost_data)
costs_df = costs_df.drop(['Conflict Probability'], axis=1)
costs_df = costs_df.T
costs_df.columns = ['p = ' + str(int(topology) / 100) for topology in topologies]
costs_df = costs_df.T
costs_df.to_csv(os.path.join(csv_dir, 'costs.csv'))
print(costs_df)

for i, algorithm in enumerate(algorithms):
    nbtr_algo = nbtr_df[nbtr_df['Algorithm'] == algorithm.upper()]
    nbtr_algo = nbtr_algo.drop(['Algorithm'], axis=1)
    nbtr_algo = nbtr_algo.drop(['Conflict Probability'], axis=1)
    nbtr_algo = nbtr_algo.T
    nbtr_algo.columns = ['p = ' + str(int(topology) / 100) for topology in topologies]
    nbtr_algo = nbtr_algo.T
    nbtr_algo.to_csv(os.path.join(csv_dir, algorithm + '_nbtr.csv'))
    print(nbtr_algo)
    graph = nbtr_algo.plot(kind='bar', rot=0)
    graph.tick_params(axis='x', labelsize=36)
    graph.tick_params(axis='y', labelsize=36)
    graph.set_xlabel('Conflict probability', fontsize=40)
    graph.set_ylabel('Node busy time ratio (%)', fontsize=40)
    graph.set_ylim([0, 100])
    graph.set_title(algorithm.upper(), fontsize=40)
    graph.legend(prop={'size': 24}, loc='upper left', bbox_to_anchor=(1, 1))
    plt.show()
success_data = []
platform_data = []
wt_data = []
ext_data = []
for topology in dataset:
    for algorithm in dataset[topology]:
        for job in dataset[topology][algorithm]:
            success_data.append((
                dataset[topology][algorithm][job]['Job Number'],
                dataset[topology][algorithm][job]['Job Type'],
                dataset[topology][algorithm][job]['Job Period'],
                dataset[topology][algorithm][job]['Target Server'],
                dataset[topology][algorithm][job]['Conflict Probability'],
                algorithm.upper(),
                dataset[topology][algorithm][job]['Job Success Rate']
            ))
            platform_data.append((
                dataset[topology][algorithm][job]['Job Number'],
                dataset[topology][algorithm][job]['Job Type'],
                dataset[topology][algorithm][job]['Job Period'],
                dataset[topology][algorithm][job]['Target Server'],
                dataset[topology][algorithm][job]['Conflict Probability'],
                algorithm.upper(),
                dataset[topology][algorithm][job]['Average Platform Delay']
            ))
            wt_data.append((
                dataset[topology][algorithm][job]['Job Number'],
                dataset[topology][algorithm][job]['Job Type'],
                dataset[topology][algorithm][job]['Job Period'],
                dataset[topology][algorithm][job]['Target Server'],
                dataset[topology][algorithm][job]['Conflict Probability'],
                algorithm.upper(),
                dataset[topology][algorithm][job]['Average Waiting Time']
            ))

success_df = pd.DataFrame(
    success_data,
    columns=['Job Number', 'Job Type', 'Job Period', 'Target Server', 'Conflict Probability', 'Algorithm',
             'Job Success Rate']
)

platform_df = pd.DataFrame(
    platform_data,
    columns=['Job Number', 'Job Type', 'Job Period', 'Target Server', 'Conflict Probability', 'Algorithm',
             'Average Platform Delay']
)

wt_df = pd.DataFrame(
    wt_data,
    columns=['Job Number', 'Job Type', 'Job Period', 'Target Server', 'Conflict Probability', 'Algorithm',
             'Average Waiting Time']
)

bar_colors = ['#32a8a6', '#eef20a', '#f2210a', '#538c53'][:len(algorithms)]

success_df['Conflict Probability'] = success_df['Conflict Probability'].map(lambda p: 'p = ' + str(p))
platform_df['Conflict Probability'] = platform_df['Conflict Probability'].map(lambda p: 'p = ' + str(p))
wt_df['Conflict Probability'] = wt_df['Conflict Probability'].map(lambda p: 'p = ' + str(p))

p1 = success_df[['Conflict Probability', 'Algorithm', 'Job Success Rate']] \
    .groupby(['Conflict Probability', 'Algorithm']).mean()
graph = p1.unstack('Algorithm')['Job Success Rate'].plot(kind='bar', color=bar_colors, rot=0)
graph.tick_params(axis='x', labelsize=36)
graph.tick_params(axis='y', labelsize=36)
graph.set_ylabel('Job Success Rate (%)', fontsize=40)
graph.set_xlabel('Conflict probability', fontsize=40)
graph.legend(prop={'size': 24}, loc='upper left', bbox_to_anchor=(1, 1))
plt.show()

p2 = platform_df[['Conflict Probability', 'Algorithm', 'Average Platform Delay']].groupby(
    ['Conflict Probability', 'Algorithm']).mean()
graph = p2.unstack('Algorithm')['Average Platform Delay'].plot(kind='bar', color=bar_colors, rot=0)
graph.tick_params(axis='x', labelsize=36)
graph.tick_params(axis='y', labelsize=36)
graph.set_ylabel('Average Platform Delay (min)', fontsize=40)
graph.set_xlabel('Conflict probability', fontsize=40)
graph.legend(prop={'size': 24}, loc='upper left', bbox_to_anchor=(1, 1))
plt.show()

p3 = wt_df[['Conflict Probability', 'Algorithm', 'Average Waiting Time']].groupby(
    ['Conflict Probability', 'Algorithm']).mean()
graph = p3.unstack('Algorithm')['Average Waiting Time'].plot(kind='bar', color=bar_colors, rot=0)
graph.tick_params(axis='x', labelsize=36)
graph.tick_params(axis='y', labelsize=36)
graph.set_ylabel('Average Waiting Time (min)', fontsize=40)
graph.set_xlabel('Conflict probability', fontsize=40)
graph.legend(prop={'size': 24}, loc='upper left', bbox_to_anchor=(1, 1))
plt.show()

p2.to_csv(os.path.join(csv_dir, 'apd.csv'))
p3.to_csv(os.path.join(csv_dir, 'awt.csv'))