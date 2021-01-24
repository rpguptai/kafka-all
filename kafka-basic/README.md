# kafka-all
The aim of this repository is to run kafka cluster locally and implement advanced concept and framework


docker commands:

To start cluster:

docker-compose up -d

To check:

docker-compose ps


list all container

docker container ls -a -q

Stop all

docker container stop $(docker container ls -a -q )

Free the memory:

docker system prune -a -f --volumes

Note : if you are running the cluster on windows

update etc/hosts

127.0.0.1	localhost
127.0.0.1 zookeeper-1 zookeeper-2 zookeeper-3
127.0.0.1 kafka-1 kafka-2 kafka-3



### verify memory and cpu usage
docker ps -q | xargs docker stats --no-stream

### verify status

docker inspect <CONTAINER_NAME> | jq '.[].State'

