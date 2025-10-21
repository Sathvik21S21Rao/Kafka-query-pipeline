import subprocess
import yaml
from dump_data import dump_data
from create_topics import cleanup_kafka_topics

def run_benchmark(cfg):
    num_producers = int(cfg["num_producers"])
    num_consumers = int(cfg["num_consumers"])

    throughputs = cfg["throughputs"]
    for throughput in throughputs:

        dump_data()
        cleanup_kafka_topics()
        
        producer_processes = []
        for i in range(num_producers):
            p = subprocess.Popen(['python', '-m', 'benchmark.Procs.producer', str(i), str(throughput//num_producers)])
            producer_processes.append(p)
        # Start consumer processes
        consumer_processes = []
        for i in range(num_consumers):
            p = subprocess.Popen(['python', '-m', 'benchmark.Procs.db_insert', str(i), str(throughput//num_producers)])
            consumer_processes.append(p)
        # Start global watermark process
        watermark_process = subprocess.Popen(['python', '-m', 'benchmark.Procs.global_watermark'])
        # Start query process
        query_process = subprocess.Popen(['python', '-m', 'benchmark.Procs.query'])
        # Wait for all processes to complete
        for p in producer_processes:
            p.wait()
        for p in consumer_processes:
            p.wait()
        watermark_process.wait()
        query_process.wait()
        result_process = subprocess.Popen(['python', '-m', 'benchmark.Procs.result'])
        result_process.wait()


if __name__ == "__main__":
    with open('./config.yml') as f:
        cfg = yaml.safe_load(f)
    run_benchmark(cfg)