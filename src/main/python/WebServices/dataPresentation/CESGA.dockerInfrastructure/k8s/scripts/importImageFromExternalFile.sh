#!/bin/bash

FILE=../exportImage/datapresentation.tar

docker load --input $FILE

docker image ls | grep datapresentation