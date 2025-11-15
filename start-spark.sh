export SPARK_WORKER_INSTANCES=2
mkdir -p /tmp/spark-events
mkdir -p /tmp/spark/checkpoints
/opt/spark/sbin/start-master.sh
/opt/spark/sbin/start-worker.sh spark://ubuntu:7077 -m 2048M --cores 2 
