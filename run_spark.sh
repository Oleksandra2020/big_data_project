docker run -d --name spark --network general-network \
	  -e SPARK_MODE=master \
	  -e SPARK_RPC_AUTHENTICATION_ENABLED=no \
	  -e SPARK_RPC_ENCRYPTION_ENABLED=no \
	  -e SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no \
	  -e SPARK_SSL_ENABLED=no bitnami/spark:3

docker run -d --name spark-worker --network general-network \
	  -e SPARK_MODE=worker \
      -e SPARK_MASTER_URL=spark://spark:7077 \
      -e SPARK_WORKER_MEMORY=1G \
      -e SPARK_WORKER_CORES=1 \
      -e SPARK_RPC_AUTHENTICATION_ENABLED=no \
      -e SPARK_RPC_ENCRYPTION_ENABLED=no \
      -e SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no \
      -e SPARK_SSL_ENABLED=no bitnami/spark:3