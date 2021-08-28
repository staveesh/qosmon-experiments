import os
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

experiment_id = 'tcp_topo_aware_40_jobs_remote_ryu'
data_path = '/home/taveesh/Desktop/qosmon_data'

controller_address = 'http://jchavula-1.cs.uct.ac.za:7800'

if not os.path.isdir(os.path.join(data_path, experiment_id)):
    os.mkdir(os.path.join(data_path, experiment_id))

algorithms = ['rr', 'edf', 'dosd', 'aosd']
docker_client = docker.from_env()

topology_file = open('net.json')
topo = json.load(topology_file)
n_hosts = topo['nHosts']
n_targets = topo['nTargets']
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
    'host': '192.168.1.76',
    'username': 'sdn',
    'password': 'containernet'
}

try:
    idx = 0
    repeat = False
    from_algo = 'rr'
    for algorithm in algorithms:
        # Server side
        print('#' * 40)
        print('Running {} on measurement server...'.format(algorithm.upper()))
        env_vars['SCHEDULING_ALGO_NAME'] = algorithm
        backend = docker_client.networks.create('backend', driver='bridge')
        influx_container = docker_client.containers.run("influxdb:1.8", detach=True, name='influxdb',
                                                        environment={'INFLUXDB_DB': 'mobiperf',
                                                                     'INFLUXDB_USER': 'root',
                                                                     'INFLUXDB_USER_PASSWORD': 'root'},
                                                        network='backend',
                                                        ports={'8086/tcp': 8086},
                                                        volumes={
                                                            '/etc/timezone': {'bind': '/etc/timezone', 'mode': 'ro'},
                                                            '/etc/localtime': {'bind': '/etc/localtime', 'mode': 'ro'},
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
                                                           '/etc/timezone': {'bind': '/etc/timezone', 'mode': 'ro'},
                                                           '/etc/localtime': {'bind': '/etc/localtime', 'mode': 'ro'},
                                                           '/home/taveesh/Desktop/qosmon/mongodb': {'bind': '/data/db',
                                                                                                    'mode': 'rw'}})
        sleep(20)
        app_container = docker_client.containers.run("staveesh/qosmon:latest", detach=True, name='app',
                                                     environment=env_vars, network='backend',
                                                     ports={'7800/tcp': 7800},
                                                     volumes={'/etc/timezone': {'bind': '/etc/timezone', 'mode': 'ro'},
                                                              '/etc/localtime': {'bind': '/etc/localtime',
                                                                                 'mode': 'ro'}})

        print('Server side docker containers started...')
        print('Starting Ryu remote controller...')
        r = requests.get(controller_address + '/start')
        print(r)
        # Client side
        print('Launching VirtualBox....')
        os.system('VBoxManage startvm "SDN" --type separate')
        sleep(60)
        print('Attempting to connect to VirtualBox instance...')

        session = paramiko.SSHClient()
        session.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        session.connect(hostname=containernet['host'],
                        username=containernet['username'],
                        password=containernet['password'])
        print('Connected')
        print('Copying topology file...')
        sftp = session.open_sftp()
        sftp.put(os.path.join(os.getcwd(), 'net.json'), '/home/' + containernet['username'] + '/net.json')
        sleep(10)
        sftp.close()
        print('Initiating python script....')
        stdin, stdout, stderr = session.exec_command("sudo -S -p '' python topology.py ")
        stdin.write(containernet['password'] + '\n')
        stdin.flush()
        sleep(30)
        print("Scheduling jobs...")
        if idx == 0:
            if repeat:
                scheduler.repeat_with_data(os.path.join(data_path, experiment_id, 'exp_' + from_algo),
                                           os.path.join(data_path, experiment_id), 'exp_' + algorithm)
            else:
                scheduler.schedule('exp_' + algorithm, os.path.join(data_path, experiment_id), n_targets)
        else:
            scheduler.repeat_with_data(os.path.join(data_path, experiment_id, 'exp_' + algorithms[idx - 1]),
                                       os.path.join(data_path, experiment_id), 'exp_' + algorithm)
        sleep(4200)  # 1 hr 10 minutes
        # Cleanup
        print('Collecting data...')
        collector.collect(os.path.join(data_path, experiment_id), 'exp_' + algorithm)
        sleep(10)
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
            sleep(10)
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
        sleep(10)
        command = "sudo docker stop "
        command += " ".join(["mn.t" + str(i) for i in range(1, n_targets + 1)])
        command += " "
        command += " ".join(["mn.h" + str(i) for i in range(1, n_hosts + 1)])
        print(command)
        stdin, stdout, stderr = session.exec_command(command, get_pty=True)
        stdin.write(containernet['password'] + '\n')
        stdin.flush()
        print(stdout.read().decode())
        err = stderr.read().decode()
        if err:
            print(err)
        sleep(10)
        command = "sudo docker rm "
        command += " ".join(["mn.t" + str(i) for i in range(1, n_targets + 1)])
        command += " "
        command += " ".join(["mn.h" + str(i) for i in range(1, n_hosts + 1)])
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

except:
    print('Stopping remote Ryu controller...')
    r = requests.get(controller_address + '/end')
    print(r)
