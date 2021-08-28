import json
import os
import pandas as pd
import matplotlib.pyplot as plt
from dateutil import parser


def minmax(a, b):
    if a <= b:
        return a, b
    return b, a


def check_overlap(int1, int2):
    x1, y1 = minmax(int1[0], int2[0])
    x2, y2 = minmax(int1[1], int2[1])
    return x1 <= y2 and y1 <= x2


data_path = '/home/taveesh/Desktop/qosmon_data/'
experiment_id = 'tcp_4_nodes_40_jobs_remote_ryu_max'
algorithms = ['random', 'rr', 'edf', 'dosd', 'aosd']
dfs = []
mx = 0
for algorithm in algorithms:
    print(algorithm)
    df = pd.read_csv(os.path.join(data_path, experiment_id, 'influx_results_' + algorithm + '.csv'), parse_dates=True)
    df['median_speed'] = df['median_speed'] / 1024
    df['time'] = df['time'].apply(lambda x: parser.parse(x))
    df['time'] = df['time'] - df.loc[0, 'time']
    df['time'] = df['time'].apply(lambda x: x.total_seconds())
    df['error'] = 100 * (10.0 - df['median_speed']) / 10.0
    df['algo'] = algorithm
    df['exp_end'] = df['exp_start'] + df['duration']
    overlaps = 0
    for idx1, row1 in df.iterrows():
        mx = max(mx, row1['duration'])
        for idx2, row2 in df.iterrows():
            if idx1 != idx2 and row1['device_id'] == row2['device_id'] and check_overlap(
                    (row1['exp_start'], row1['exp_end']), (row2['exp_start'], row2['exp_end'])):
                overlaps += 1
    overlaps /= 2
    print('Overlaps = {}'.format(overlaps))
print('Max = {}'.format(mx))
# final_df = pd.concat(dfs)
# final_df[['algo', 'error']].groupby(['algo']).mean().plot(kind='bar')
# plt.show()
