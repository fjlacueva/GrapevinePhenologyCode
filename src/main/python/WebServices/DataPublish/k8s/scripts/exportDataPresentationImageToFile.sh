#!/bin/bash

mkdir ../exportImage -p
FILE=../exportImage/datapublishmanager.tar
IMAGE=datapublishmanager
TAG=1.0.1

docker save --output=$FILE $IMAGE:$TAG