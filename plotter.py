import pandas as pd
import matplotlib.pyplot as plt
import os
import json
from functools import reduce

params = {'legend.fontsize': 'x-large',
          'figure.figsize': (15, 5),
          'axes.labelsize': 'x-large',
          'axes.titlesize': 'x-large',
          'xtick.labelsize': 'x-large',
          'ytick.labelsize': 'x-large'}

TAGS = ['exp_rr', 'exp_edf', 'exp_dosd', 'exp_aosd']

misses = []
delays = []
waiting_times = []
busy_times = []
algos = []

for tag in TAGS:
    algo = tag.split('_')[1].upper()
    algos.append(algo)
    # Job misses
    misses.append(pd.read_csv(tag+'_analysis/job_misses.csv'))
    misses[-1] = misses[-1].drop('job_key', axis=1)
    if len(misses) > 1:
        misses[-1] = misses[-1].drop('total_expected_instances', axis=1)
    else:
        misses[-1] = misses[-1].rename(columns={'total_expected_instances': 'Total instances'})
    misses[-1] = misses[-1].rename(columns={'total_instances_ran': algo})
    # Platform delays
    delays.append(pd.read_csv(tag+'_analysis/average_platform_delay.csv'))
    delays[-1] = delays[-1].drop('job_key', axis=1)
    delays[-1] = delays[-1].rename(columns={'average_delay_min': algo})
    # Average waiting times
    waiting_times.append(pd.read_csv(tag+'_analysis/average_waiting_times.csv'))
    waiting_times[-1] = waiting_times[-1].rename(columns={'average_waiting_time': algo})

    busy_times.append(pd.read_csv(tag+'_analysis/total_busy_times.csv'))

job_request_dict = {}

for data_file in os.listdir(TAGS[0]):
    job_key = data_file.replace('.json', '')
    with open(os.path.join(TAGS[0], data_file), "r") as fd:
        job_request_dict[job_key] = json.load(fd)

df_misses_merged = reduce(lambda left, right: pd.merge(left,right, on='job_number', how='inner'), misses)
print(df_misses_merged.columns)
df_misses_merged = df_misses_merged.drop('Unnamed: 0_x', axis=1)
df_misses_merged = df_misses_merged.drop('Unnamed: 0_y', axis=1)
df_misses_merged['Job #'] = ['Job ' + str(i) for i in df_misses_merged['job_number']]
df_misses_merged = df_misses_merged.drop('job_number', axis=1)
df_misses_merged = df_misses_merged[['Total instances'] + algos + ['Job #']]
graph = df_misses_merged.plot(kind='bar', lw=2, colormap='jet')
graph.set_ylabel('Number of instances')
plt.xticks(range(0, len(df_misses_merged)), df_misses_merged['Job #'], rotation=45)
plt.grid()
plt.show()

df_delay_merged = reduce(lambda left, right: pd.merge(left,right, on='job_number', how='inner'), delays)
df_delay_merged = df_delay_merged.drop('Unnamed: 0_x', axis=1)
df_delay_merged = df_delay_merged.drop('Unnamed: 0_y', axis=1)
df_delay_merged['Job #'] = ['Job ' + str(i) for i in df_delay_merged['job_number']]
df_delay_merged = df_delay_merged.drop('job_number', axis=1)
df_delay_merged = df_delay_merged[algos + ['Job #']]
graph = df_delay_merged.plot(kind='bar', lw=2, colormap='jet')
graph.set_ylabel('Average platform delay (min)')
plt.xticks(range(0, len(df_delay_merged)), df_delay_merged['Job #'], rotation=45)
plt.grid()
plt.show()

df_waiting_times_merged = reduce(lambda left, right: pd.merge(left,right, on='job_number', how='inner'), waiting_times)
df_waiting_times_merged = df_waiting_times_merged.drop('Unnamed: 0_x', axis=1)
df_waiting_times_merged = df_waiting_times_merged.drop('Unnamed: 0_y', axis=1)
df_waiting_times_merged['Job #'] = ['Job ' + str(i) for i in df_waiting_times_merged['job_number']]
df_waiting_times_merged = df_waiting_times_merged.drop('job_number', axis=1)
df_waiting_times_merged = df_waiting_times_merged[algos + ['Job #']]
graph = df_waiting_times_merged.plot(kind='bar', lw=2, colormap='jet')
graph.set_ylabel('Average waiting time (min)')
plt.xticks(range(0, len(df_waiting_times_merged)), df_waiting_times_merged['Job #'], rotation=45)
plt.grid()
plt.show()

#
# df_misses_merged.columns = ['DOSD_ran', 'Total instances', 'rr_ran', 'Job #']
# df_delay_merged.columns = ['DOSD_delay', 'rr_delay', 'Job #']
# df_overall = pd.merge(df_misses_merged, df_delay_merged, on='Job #', how='inner')
# job_types = [job_request_dict['exp_dosd_job_' + str(i + 1)]['jobDescription']['measurementDescription']['type'] for i in
#              range(20)]
# df_overall['type'] = job_types
# df_overall_grouped = df_overall.groupby(['type']).mean()
# df_overall_grouped['type'] = ['dns_lookup', 'http', 'ping', 'tcp_speed_test', 'traceroute']
# df_final = df_overall_grouped[['type', 'DOSD_delay', 'rr_delay']]
# graph = df_final.plot(kind='bar', lw=2, colormap='jet')
# graph.set_ylabel('Average platform delay')
# graph.set_xlabel(None)
# plt.xticks(range(0, len(df_final)), df_final['type'], rotation=0)
# plt.grid()
# plt.show()
#
# df_awt_merged = pd.merge(df_dosd_waiting_times, df_rr_waiting_times, on='job_number', how='inner')
# df_awt_merged = df_awt_merged.drop('Unnamed: 0_x', axis=1)
# df_awt_merged = df_awt_merged.drop('Unnamed: 0_y', axis=1)
# df_awt_merged['Job #'] = ['Job ' + str(i) for i in df_awt_merged['job_number']]
# df_awt_merged = df_awt_merged.drop('job_number', axis=1)
# df_awt_merged.columns = ['DOSD', 'rr', 'Job #']
# graph = df_awt_merged.plot(kind='bar', lw=2, colormap='jet')
# graph.set_ylabel('Average waiting time')
# graph.set_xlabel(None)
# plt.xticks(range(0, len(df_awt_merged)), df_awt_merged['Job #'], rotation=45)
# plt.grid()
# plt.show()
#
# df_bt_merged = pd.merge(df_dosd_bt, df_rr_bt, on='node_id', how='inner')
# df_bt_merged = df_bt_merged.drop('Unnamed: 0_x', axis=1)
# df_bt_merged = df_bt_merged.drop('Unnamed: 0_y', axis=1)
# df_bt_merged['Node'] = ['Node ' + str(i) for i in range(1, 5)]
# df_bt_merged = df_bt_merged.drop('node_id', axis=1)
# print(df_bt_merged)
# df_bt_merged.columns = ['DOSD', 'rr', 'Node']
# df_bt_merged['DOSD'] = df_bt_merged['dosd'] / (2 * 60 * 60 * 1000)
# df_bt_merged['rr'] = df_bt_merged['rr'] / (2 * 60 * 60 * 1000)
# graph = df_bt_merged.plot(kind='bar', lw=2, colormap='jet')
# graph.set_ylabel('Node busy time ratio')
# graph.set_xlabel(None)
# plt.xticks(range(0, len(df_bt_merged)), df_bt_merged['Node'], rotation=45)
# plt.grid()
# plt.show()
