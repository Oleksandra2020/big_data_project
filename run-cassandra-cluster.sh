docker network create general-network

docker run --name cassandra-node1 --network general-network -d cassandra:latest

sleep 60