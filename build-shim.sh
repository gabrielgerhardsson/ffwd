#!/bin/bash

timestamp=$(date +"%Y%m%dT%H%M%S")
tag=gcr.io/operation-covfefe-1/ffwd-java-shim:$timestamp

gcloud docker -- build -t $tag -f docker/Dockerfile.shim .
gcloud docker -- push $tag
