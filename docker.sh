#!/bin/bash

sbt docker:publishLocal
docker images -q --filter "dangling=true" | xargs docker rmi

data_dir=$PWD/target/data
mkdir -p $data_dir


docker run --name=unicorn-docker -hostname=unicorn-docker --volume=$data_dir:/data -it --rm unicorn:1.2.0
