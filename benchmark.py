import subprocess
from subprocess import DEVNULL
import yaml
from dump_data import dump_data
from create_topics import create_kafka_topics
import time
def run_benchmark(cfg):
    num_producers = int(cfg["num_producers"])
    num_consumers = int(cfg["num_consumers"])
    proc_type = cfg["proc_type"].upper()
    #dump_data(proc_type=proc_type.lower())
    create_kafka_topics(topics=cfg[cfg['proc_type']]['topics'],bootstrap_servers=cfg['bootstrap_servers'])
    throughputs = cfg["throughputs"]



    for throughput in throughputs:

        
        # Start consumer processes
        consumer_processes = []
        if proc_type == "SQL":
            for i in range(num_consumers):
                p = subprocess.Popen(['python', '-m', f'benchmark.{proc_type}Procs.db_insert', str(i), str(throughput//num_producers)])
                consumer_processes.append(p)
            watermark_process = subprocess.Popen(['python', '-m', f'benchmark.{proc_type}Procs.global_watermark'])
            query_process = subprocess.Popen(['python', '-m', f'benchmark.{proc_type}Procs.query'])
            
            for p in consumer_processes:
                p.wait()
            watermark_process.wait()
            query_process.wait()

            

        producer_processes = []


        for i in range(num_producers):
            p = subprocess.Popen(['python', '-m', 'benchmark.producer', str(i), str(throughput//num_producers)])
            producer_processes.append(p)
        for p in producer_processes:
            p.wait()

        #result_process = subprocess.Popen(['python', '-m', f'benchmark.{proc_type}Procs.result'])
        #result_process.wait()
        


if __name__ == "__main__":
    with open('./config.yml') as f:
        cfg = yaml.safe_load(f)
    run_benchmark(cfg)
