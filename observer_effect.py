import pandas as pd
import os
import matplotlib.pyplot as plt

plt.style.use('classic')

hosts = [i for i in range(3, 10)]
algorithms = ['random', 'rr', 'edf', 'dosd', 'aosd']

data_dir = '/home/taveesh/Desktop/QoSMon/qosmon-experiments/clique_multipath_same_jobs'

baseline_speed = 10000

dataset = {}

host_error = []
for n_hosts in hosts:
    dataset[n_hosts] = {}
    os.chdir(os.path.join(data_dir, 'experiment_{}'.format(n_hosts)))
    for algo in algorithms:
        df = pd.read_csv('influx_results_' + algo + '.csv')
        df['error'] = 100 * abs(df['median_speed'] - baseline_speed) / baseline_speed
        dataset[n_hosts][algo] = df['error'].mean()
        host_error.append(dataset[n_hosts][algo])

df = pd.DataFrame(dataset)
df.T.to_csv('observer.csv')
ax = df.T.plot(kind='bar')
plt.show()