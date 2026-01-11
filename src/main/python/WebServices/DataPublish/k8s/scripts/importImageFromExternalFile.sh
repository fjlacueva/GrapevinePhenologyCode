#!/bin/bash

FILE=../exportImage/datapublishmanager.tar

docker load --input $FILE

docker image ls | grep datapublishmanager