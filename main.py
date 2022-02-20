import os
import sys
import paramiko
from time import sleep
import requests
import scheduler
import docker
import collector
from pymongo import MongoClient
from influxdb import InfluxDBClient
import pandas as pd
import json
import util


def generate_topology(num_hosts):
    result = {}
    num_switches = num_hosts + 1
    num_targets = 1
    result['nSwitches'] = num_switches
    result['nHosts'] = num_hosts
    result['nTargets'] = num_targets
    links = []
    switches = ['s' + str(i) for i in range(1, num_switches + 1)]
    i = 1
    for switch in switches:
        neighbors = []
        for switch2 in switches:
            if switch2 != switch:
                neighbors.append(switch2)
        if i < num_switches:
            neighbors.append('h' + switch[1:])
        else:
            neighbors.append('t1')
        links.append({
            'node': switch,
            'neighbors': neighbors
        })
        i += 1
    result['links'] = links
    result['delay'] = '10ms'
    result['bandwidth'] = 10
    print(result)
    with open('topos/topo-{}.json'.format(num_hosts), 'w+') as fp:
        json.dump(result, fp)


data_path = 'qosmon_data'
if not os.path.isdir(data_path):
    util.create_dir(data_path)

if not os.path.isdir('qosmon'):
    util.create_dir('qosmon')

controller_address = 'http://jchavula-1.cs.uct.ac.za:7800'

env_vars = {
    'INFLUXDB_USERNAME': 'root',
    'INFLUXDB_PASSWORD': 'root',
    'INFLUXDB_NAME': 'mobiperf',
    'INFLUXDB_PORT': 8086,
    'MONGODB_USERNAME': 'root',
    'MONGODB_PASSWORD': 'root',
    'MONGODB_NAME': 'qosmon',
    'MONGODB_PORT': 27017,
    'MONGODB_HOST': 'mongodb',
    'HTTP_SERVER_PORT': 7800,
    'FILE_SERVER_HOSTNAME': '',
    'MILLISECONDS_TILL_RETRY_CONNECT': 120000,
    'MILLISECONDS_TILL_ANALYZE_PCAPFILES': 86400000,
    'MILLISECONDS_INIT_DELAY': 60000,
    'FILE_MONITOR_DELAY': 5000,
    'NUM_RETRY_CONNECT': 3,
    'SCHEDULING_ALGO_NAME': ''
}
containernet = {
    'host': '137.158.58.212',
    'username': 'sdn',
    'password': 'containernet'
}
algorithms = ['random', 'rr', 'edf', 'dosd', 'aosd']
n_jobs = 50

topos = range(10, 16)

exp_config = [(x, y) for x in topos for y in algorithms]

current_directory = '/home/taveesh/Desktop/qosmon-experiments'


def go():
    exp_idx = 0
    os.system('docker stop app influxdb mongodb')
    os.system('docker rm app influxdb mongodb')
    os.system('docker network rm backend')
    print('Stopping remote Ryu controller...')
    r = requests.get(controller_address + '/end')
    print(r)
    topo_id = topos[0]
    for exp in os.listdir(data_path):
        topo_id = max(topo_id, int(exp.split('_')[1]))
    print('topo_id = ', topo_id)
    new_exp = os.path.join(data_path, 'experiment_{}'.format(str(topo_id)))
    if os.path.exists(new_exp):
        os.chdir(new_exp)
        result_files = [exp_file for exp_file in os.listdir(os.getcwd()) if exp_file.startswith('influx')]
        offset = -1
        for algo in algorithms:
            file_name = 'influx_results_' + algo + '.csv'
            if file_name not in result_files:
                break
            offset += 1

        if offset == -1:
            topo_id += 1
            offset += 1

        exp_idx = (topo_id - topos[0]) * len(algorithms) + offset + 1
    else:
        exp_idx = 0
    os.chdir(current_directory)

    print('Resuming experiments from index : ', exp_idx)

    for experiment in exp_config[exp_idx:]:
        topo = experiment[0]
        algorithm = experiment[1]
        experiment_id = 'experiment_{}'.format(topo)
        if not os.path.isdir(os.path.join(data_path, experiment_id)):
            util.create_dir(os.path.join(data_path, experiment_id))
        docker_client = docker.from_env()
        n_targets = 1
        idx = algorithms.index(algorithm)
        # Server side
        print('#' * 40)
        print('Configuration : n_devices = {} algo = {}'.format(topo, algorithm.upper()))
        env_vars['SCHEDULING_ALGO_NAME'] = algorithm
        backend = docker_client.networks.create('backend', driver='bridge')
        influx_container = docker_client.containers.run("influxdb:1.8", detach=True, name='influxdb',
                                                        environment={'INFLUXDB_DB': 'mobiperf',
                                                                     'INFLUXDB_USER': 'root',
                                                                     'INFLUXDB_USER_PASSWORD': 'root'},
                                                        network='backend',
                                                        ports={'8086/tcp': 8086},
                                                        volumes={
                                                            '/etc/timezone': {'bind': '/etc/timezone',
                                                                              'mode': 'ro'},
                                                            '/etc/localtime': {'bind': '/etc/localtime',
                                                                               'mode': 'ro'},
                                                            '/home/taveesh/Desktop/qosmon/influxdb': {
                                                                'bind': '/var/lib/influxdb',
                                                                'mode': 'rw'}})

        mongo_container = docker_client.containers.run("mongo:latest", detach=True, name='mongodb',
                                                       environment={'MONGO_INITDB_DATABASE': 'qosmon',
                                                                    'MONGO_INITDB_ROOT_USERNAME': 'root',
                                                                    'MONGO_INITDB_ROOT_PASSWORD': 'root'},
                                                       network='backend',
                                                       ports={'27017/tcp': 27017},
                                                       volumes={
                                                           '/etc/timezone': {'bind': '/etc/timezone',
                                                                             'mode': 'ro'},
                                                           '/etc/localtime': {'bind': '/etc/localtime',
                                                                              'mode': 'ro'},
                                                           '/home/taveesh/Desktop/qosmon/mongodb': {
                                                               'bind': '/data/db',
                                                               'mode': 'rw'}})
        sleep(120)
        app_container = docker_client.containers.run("staveesh/qosmon:latest", detach=True, name='app',
                                                     environment=env_vars, network='backend',
                                                     ports={'7800/tcp': 7800},
                                                     volumes={'/etc/timezone': {'bind': '/etc/timezone',
                                                                                'mode': 'ro'},
                                                              '/etc/localtime': {'bind': '/etc/localtime',
                                                                                 'mode': 'ro'}})

        print('Server side docker containers started...')
        print('Starting Ryu remote controller...')
        r = requests.get(controller_address + '/start')
        print(r)
        # Client side
        # print('Launching VirtualBox....')
        # os.system('VBoxManage startvm "SDN" --type separate')
        sleep(120)
        # print('Attempting to connect to VirtualBox instance...')
        #
        # session = paramiko.SSHClient()
        # session.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # session.connect(hostname=containernet['host'],
        #                 username=containernet['username'],
        #                 password=containernet['password'])
        # print('Connected')
        # print('Copying topology file...')
        # sftp = session.open_sftp()
        # sftp.put(os.path.join(os.getcwd(), 'topos/topo-{}.json'.format(topo)),
        #          '/home/' + containernet['username'] + '/topo-{}.json'.format(topo))
        # sleep(120)
        # sftp.close()
        # print('Cleaning mininet')
        # stdin, stdout, stderr = session.exec_command("sudo -S -p '' mn -c")
        # stdin.write(containernet['password'] + '\n')
        # stdin.flush()
        # sleep(120)
        # print('Initiating python script....')
        # stdin, stdout, stderr = session.exec_command(
        #     "sudo -S -p '' python topology.py {}".format('topo-{}.json'.format(topo)))
        # stdin.write(containernet['password'] + '\n')
        # stdin.flush()
        # sleep(120)
        print("Scheduling jobs...")
        scheduler.repeat_with_data(os.path.join(current_directory, 'jobs', 'exp_job'),
                                   os.path.join(data_path, experiment_id), 'exp_' + algorithm)
        sleep(11400)  # 3 hr 10 minutes
        # Cleanup
        print('Collecting data...')
        collector.collect(os.path.join(data_path, experiment_id), 'exp_' + algorithm)
        sleep(120)
        influx_client = InfluxDBClient(host='localhost', port=8086)
        influx_client.switch_database('mobiperf')
        measurements = [dct['name'] for dct in influx_client.get_list_measurements()]
        if 'tcp_speed_test' in measurements:
            results = influx_client.query('select * from tcp_speed_test')
            points = []
            for point in results.get_points():
                points.append(point)
            df = pd.DataFrame(points)
            df.to_csv(os.path.join(data_path, experiment_id, 'influx_results_' + algorithm + '.csv'))
            sleep(120)
        print('Cleaning up influxdb...')
        influx_client.query('DROP SERIES FROM /.*/')
        influx_client.close()
        print('Cleaning up mongodb...')
        mongo_client = MongoClient('mongodb://root:root@localhost:27017/')
        mongo_client.drop_database('qosmon')
        print('Time to exit...')
        session = paramiko.SSHClient()
        session.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        session.connect(hostname=containernet['host'],
                        username=containernet['username'],
                        password=containernet['password'])
        print("sudo -S pkill -f 'topology.py'")
        stdin, stdout, stderr = session.exec_command("sudo -S pkill -f 'topology.py'", get_pty=True)
        stdin.write(containernet['password'] + '\n')
        stdin.flush()
        print(stdout.read().decode())
        err = stderr.read().decode()
        if err:
            print(err)
        print("sudo mn -c")
        stdin, stdout, stderr = session.exec_command("sudo mn -c", get_pty=True)
        stdin.write(containernet['password'] + '\n')
        stdin.flush()
        print(stdout.read().decode())
        err = stderr.read().decode()
        if err:
            print(err)
        sleep(120)
        command = "sudo docker stop "
        command += " ".join(["mn.t" + str(i) for i in range(1, n_targets + 1)])
        command += " "
        command += " ".join(["mn.h" + str(i) for i in range(1, topo + 1)])
        print(command)
        stdin, stdout, stderr = session.exec_command(command, get_pty=True)
        stdin.write(containernet['password'] + '\n')
        stdin.flush()
        print(stdout.read().decode())
        err = stderr.read().decode()
        if err:
            print(err)
        sleep(120)
        command = "sudo docker rm "
        command += " ".join(["mn.t" + str(i) for i in range(1, n_targets + 1)])
        command += " "
        command += " ".join(["mn.h" + str(i) for i in range(1, topo + 1)])
        print(command)
        stdin, stdout, stderr = session.exec_command(command, get_pty=True)
        stdin.write(containernet['password'] + '\n')
        stdin.flush()
        print(stdout.read().decode())
        err = stderr.read().decode()
        if err:
            print(err)
        session.close()
        print('Stopping remote Ryu controller...')
        r = requests.get(controller_address + '/end')
        print(r)
        influx_container.stop()
        mongo_container.stop()
        app_container.stop()
        influx_container.remove()
        mongo_container.remove()
        app_container.remove()
        backend.remove()
        os.system('VBoxManage controlvm "SDN" poweroff')
        idx += 1
    #
    # except:
    #     print('Stopping remote Ryu controller...')
    #     r = requests.get(controller_address + '/end')
    #     print(r)
    #     sys.exit()


if __name__ == '__main__':
    # scheduler.schedule('exp_job', os.path.join(data_path, 'jobs'), 1, n_jobs)
    go()
