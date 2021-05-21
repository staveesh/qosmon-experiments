import pandas as pd
import matplotlib.pyplot as plt
import os
import json
import matplotlib.pylab as pylab
params = {'legend.fontsize': 'x-large',
          'figure.figsize': (15, 5),
         'axes.labelsize': 'x-large',
         'axes.titlesize':'x-large',
         'xtick.labelsize':'x-large',
         'ytick.labelsize':'x-large'}

df_rr_misses = pd.read_csv('exp_rr_analysis/job_misses.csv')
df_edf_misses = pd.read_csv('exp_edf_analysis/job_misses.csv')
df_rr_delay = pd.read_csv('exp_rr_analysis/average_platform_delay.csv')
df_edf_delay = pd.read_csv('exp_edf_analysis/average_platform_delay.csv')
df_rr_waiting_times = pd.read_csv('exp_rr_analysis/average_waiting_times.csv')
df_edf_waiting_times = pd.read_csv('exp_edf_analysis/average_waiting_times.csv')
df_rr_bt = pd.read_csv('exp_rr_analysis/total_busy_times.csv')
df_edf_bt = pd.read_csv('exp_edf_analysis/total_busy_times.csv')

job_request_dict = {}

for data_file in os.listdir('exp_rr'):
    job_key = data_file.replace('.json', '')
    with open(os.path.join('exp_rr', data_file), "r") as fd:
        job_request_dict[job_key] = json.load(fd)

# df_misses_merged = pd.merge(df_rr_misses, df_edf_misses, on='job_number', how='inner')
# df_misses_merged = df_misses_merged.drop('Unnamed: 0_x', axis=1)
# df_misses_merged = df_misses_merged.drop('Unnamed: 0_y', axis=1)
# df_misses_merged = df_misses_merged.drop('job_key_x', axis=1)
# df_misses_merged = df_misses_merged.drop('job_key_y', axis=1)
# df_misses_merged = df_misses_merged.drop('total_expected_instances_x', axis=1)
# df_misses_merged['Job #'] = ['Job ' + str(i) for i in df_misses_merged['job_number']]
# df_misses_merged = df_misses_merged.drop('job_number', axis=1)
# df_misses_merged.columns = ['RR', 'Total instances', 'EDF', 'Job #']
# df_misses_merged = df_misses_merged[['Total instances', 'RR', 'EDF', 'Job #']]
# graph = df_misses_merged.plot(kind='bar', lw=2, colormap='jet')
# graph.set_ylabel('Number of instances')
# plt.xticks(range(0, len(df_misses_merged)), df_misses_merged['Job #'], rotation=45)
# plt.grid()
# plt.show()
#
# df_delay_merged = pd.merge(df_rr_delay, df_edf_delay, on='job_number', how='inner')
# df_delay_merged = df_delay_merged.drop('Unnamed: 0_x', axis=1)
# df_delay_merged = df_delay_merged.drop('Unnamed: 0_y', axis=1)
# df_delay_merged = df_delay_merged.drop('job_key_x', axis=1)
# df_delay_merged = df_delay_merged.drop('job_key_y', axis=1)
# df_delay_merged['Job #'] = ['Job ' + str(i) for i in df_delay_merged['job_number']]
# df_delay_merged = df_delay_merged.drop('job_number', axis=1)
# df_delay_merged.columns = ['RR', 'EDF', 'Job #']
# graph = df_delay_merged.plot(kind='bar', lw=2, colormap='jet')
# graph.set_ylabel('Average platform delay')
# plt.xticks(range(0, len(df_delay_merged)), df_delay_merged['Job #'], rotation=45)
# plt.grid()
# plt.show()
#
# df_misses_merged.columns = ['RR_ran', 'Total instances', 'EDF_ran', 'Job #']
# df_delay_merged.columns = ['RR_delay', 'EDF_delay', 'Job #']
# df_overall = pd.merge(df_misses_merged, df_delay_merged, on='Job #', how='inner')
# job_types = [job_request_dict['exp_rr_job_' + str(i + 1)]['jobDescription']['measurementDescription']['type'] for i in
#              range(20)]
# df_overall['type'] = job_types
# df_overall_grouped = df_overall.groupby(['type']).mean()
# df_overall_grouped['type'] = ['dns_lookup', 'http', 'ping', 'tcp_speed_test', 'traceroute']
# df_final = df_overall_grouped[['type', 'RR_delay', 'EDF_delay']]
# graph = df_final.plot(kind='bar', lw=2, colormap='jet')
# graph.set_ylabel('Average platform delay')
# graph.set_xlabel(None)
# plt.xticks(range(0, len(df_final)), df_final['type'], rotation=0)
# plt.grid()
# plt.show()

# df_awt_merged = pd.merge(df_rr_waiting_times, df_edf_waiting_times, on='job_number', how='inner')
# df_awt_merged = df_awt_merged.drop('Unnamed: 0_x', axis=1)
# df_awt_merged = df_awt_merged.drop('Unnamed: 0_y', axis=1)
# df_awt_merged['Job #'] = ['Job ' + str(i) for i in df_awt_merged['job_number']]
# df_awt_merged = df_awt_merged.drop('job_number', axis=1)
# df_awt_merged.columns = ['RR', 'EDF', 'Job #']
# graph = df_awt_merged.plot(kind='bar', lw=2, colormap='jet')
# graph.set_ylabel('Average waiting time')
# graph.set_xlabel(None)
# plt.xticks(range(0, len(df_awt_merged)), df_awt_merged['Job #'], rotation=45)
# plt.grid()
# plt.show()

df_bt_merged = pd.merge(df_rr_bt, df_edf_bt, on='node_id', how='inner')
df_bt_merged = df_bt_merged.drop('Unnamed: 0_x', axis=1)
df_bt_merged = df_bt_merged.drop('Unnamed: 0_y', axis=1)
df_bt_merged['Node'] = ['Node '+str(i) for i in range(1, 5)]
df_bt_merged = df_bt_merged.drop('node_id', axis=1)
print(df_bt_merged)
df_bt_merged.columns = ['RR', 'EDF', 'Node']
df_bt_merged['RR'] = df_bt_merged['RR'] / (2*60*60*1000)
df_bt_merged['EDF'] = df_bt_merged['EDF'] / (2*60*60*1000)
graph = df_bt_merged.plot(kind='bar', lw=2, colormap='jet')
graph.set_ylabel('Node busy time ratio')
graph.set_xlabel(None)
plt.xticks(range(0, len(df_bt_merged)), df_bt_merged['Node'], rotation=45)
plt.grid()
plt.show()