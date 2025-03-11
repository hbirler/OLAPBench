#!/usr/bin/env bash

# Build docker image
docker build --tag=gitlab.db.in.tum.de:5005/schmidt/olapbench/cedardb:latest --ulimit nofile=1048576:1048576 --ulimit memlock=8388608:8388608 .
docker login gitlab.db.in.tum.de:5005
docker push gitlab.db.in.tum.de:5005/schmidt/olapbench/cedardb:latest
