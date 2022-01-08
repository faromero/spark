#!/bin/bash

# Build spark
./build/sbt package

# Build and install pyspark
pushd python
python3 setup.py sdist
popd

