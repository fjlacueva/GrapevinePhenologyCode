#!/bin/bash

mkdir ../exportImage -p
FILE=../exportImage/datapresentation.tar
IMAGE=datapresentationmanager
TAG=1.0.1

docker save --output=$FILE $IMAGE:$TAG