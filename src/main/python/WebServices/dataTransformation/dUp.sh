#!/bin/bash
cd /projects/grapevine/GIT/src/src/main/python/WebServices/dataTransformation
bash launchEnv.sh
docker-compose -f docker/docker-compose.yaml up &