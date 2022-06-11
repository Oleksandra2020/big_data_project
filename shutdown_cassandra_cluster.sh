docker stop cassandra-node1
docker stop spark
docker stop spark-submit

docker rm cassandra-node1
docker rm spark
docker rm spark-submit

docker network rm general-network
