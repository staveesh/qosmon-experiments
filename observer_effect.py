import pandas as pd
import os
import statistics
import matplotlib.pyplot as plt

plt.style.use('classic')

jtds = [i for i in range(1, 11)]
hosts = [i for i in range(1, 11)]
algorithms = ['random', 'rr', 'edf', 'dosd', 'aosd']

data_dir = '/home/taveesh/Desktop/Exp_data/tcp_speed_tests_tree_topo'

max_values = []
# Baseline data
for host in hosts:
    for jtd in jtds:
        baseline_config_dir = os.path.join(data_dir, 'experiment_{}_{}'.format(host, jtd))
        os.chdir(baseline_config_dir)
        for algo in algorithms:
            df = pd.read_csv('influx_results_' + algo + '.csv')
            max_values += list(df['max_speed'])
baseline_speed = max(max_values)
print('Baseline speed = ' + str(baseline_speed))

dataset = {}
jtd_error = {}

for jtd in jtds:
    dataset[jtd] = {}
    host_error = []
    for n_hosts in hosts:
        dataset[jtd][n_hosts] = {}
        os.chdir(os.path.join(data_dir, 'experiment_{}_{}'.format(n_hosts, jtd)))
        for algo in algorithms:
            df = pd.read_csv('influx_results_' + algo + '.csv')
            df['error'] = 100 * abs(df['median_speed'] - baseline_speed) / df['median_speed']
            dataset[jtd][n_hosts][algo] = df['error'].mean()
            host_error.append(dataset[jtd][n_hosts][algo])
    jtd_error[jtd] = statistics.mean(host_error)

jtd_df = pd.DataFrame(jtd_error.items(), columns=['JTD', 'Error'])
jtd_df.plot(x = 'JTD', y='Error')
plt.show()