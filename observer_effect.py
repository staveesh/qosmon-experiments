import pandas as pd
import os
import matplotlib.pyplot as plt

plt.style.use('classic')

hosts = range(3, 11)
algorithms = ['random', 'rr', 'edf', 'dosd', 'aosd']

data_dir = os.path.join(os.getcwd(), 'phase2_final')

baseline_speed = 10000

observer = {}

host_error = []
for n_hosts in hosts:
    observer[n_hosts] = {}
    os.chdir(os.path.join(data_dir, 'experiment_{}'.format(n_hosts)))
    for algo in algorithms:
        df = pd.read_csv('influx_results_' + algo + '.csv')
        df['error'] = 100 * abs(df['median_speed'] - baseline_speed) / baseline_speed
        observer[n_hosts][algo] = df['error'].mean()
        host_error.append(observer[n_hosts][algo])

df = pd.DataFrame(observer)
df.T.to_csv('gnuplot/observer.dat', sep='\t')
ax = df.T.plot(kind='bar')
plt.show()