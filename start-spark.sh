export SPARK_WORKER_INSTANCES=1
mkdir -p /tmp/spark-events
mkdir -p /tmp/spark/checkpoints
/opt/spark/sbin/start-master.sh
/opt/spark/sbin/start-worker.sh spark://ubuntu:7077 -m 4096M --cores 4 
