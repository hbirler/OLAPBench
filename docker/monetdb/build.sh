#!/usr/bin/env bash

# Build docker image
docker build --tag=gitlab.db.in.tum.de:5005/schmidt/olapbench/monetdb:latest --ulimit nofile=2048 .
docker login gitlab.db.in.tum.de:5005
docker push gitlab.db.in.tum.de:5005/schmidt/olapbench/monetdb:latest
