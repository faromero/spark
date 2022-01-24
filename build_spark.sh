#!/bin/bash

# Build spark
./build/sbt package

# Build and install pyspark
pushd python
python3 setup.py sdist
popd

###
# Build with Kubernetes support
# ./build/mvn -Pkubernetes -DskipTests clean package

# Build PySpark container
# repo_name="faromero"
# sudo ./bin/docker-image-tool.sh -r ${repo_name} -t latest -p ./resource-managers/kubernetes/docker/src/main/dockerfiles/spark/bindings/python/Dockerfile build

# Push containers
# You likely need to do "sudo docker login" first
# sudo ./bin/docker-image-tool.sh -r ${repo_name} -t latest push

