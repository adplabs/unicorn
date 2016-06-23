#!/bin/bash
#
# Script to start docker and update the /etc/hosts file to point to
# the unicorn-docker container
#
# hbase thrift and master server logs are written to the local
# logs directory
#

echo "Starting Unicorn container"
data_dir=$PWD/data
mkdir -p $data_dir
id=$(docker run --name=unicorn-docker -h unicorn-docker -d -v $data_dir:/data unicorn)

echo "Container has ID $id"

# Get the hostname and IP inside the container
docker inspect $id > config.json
docker_hostname=`python -c 'import json; c=json.load(open("config.json")); print c[0]["Config"]["Hostname"]'`
docker_ip=`python -c 'import json; c=json.load(open("config.json")); print c[0]["NetworkSettings"]["IPAddress"]'`
rm -f config.json

echo "Updating /etc/hosts to make unicorn-docker point to $docker_ip ($docker_hostname)"
if grep 'unicorn-docker' /etc/hosts >/dev/null; then
  sudo sed -i .bak "s/^.*unicorn-docker.*\$/$docker_ip unicorn-docker $docker_hostname/" /etc/hosts
else
  sudo sh -c "echo '$docker_ip unicorn-docker $docker_hostname' >> /etc/hosts"
fi

echo "Now connect to unicorn at localhost on the standard ports"
echo "  ZK 2181, Thrift 9090, Master 16000, Region 16020"
echo "Or connect to host unicorn-docker (in the container) on the same ports"
echo ""
echo "For docker status:"
echo "$ id=$id"
echo "$ docker inspect \$id"
