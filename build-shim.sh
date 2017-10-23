#!/bin/bash

IMAGE_ID=$(docker build -f docker/Dockerfile.shim .)

echo $IMAGE_ID
