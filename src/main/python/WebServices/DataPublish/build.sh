#!/bin/bash
sudo docker rm -f pTest

mkdir -p /projects/grapevine/GIT/src/src/main/python/WebServices/DataPublish/CESGA.dockerInfrastructure/python

PYTHONPATH=/projects/grapevine/GIT/src/src/main/python/WebServices/DataPublish/
DOCKERBUILDPATH=$PYTHONPATH/CESGA.dockerInfrastructure
rsync -av --exclude=CESGA.dockerInfrastructure --exclude=build.sh --exclude=k8s $PYTHONPATH $DOCKERBUILDPATH/python

IMAGENAME=datapublishmanager
VERSION=1.0.1

cd $DOCKERBUILDPATH/python
docker build -t $IMAGENAME:latest -t $IMAGENAME:$VERSION .

echo 'FOR TESTING ON DOCKER:'
echo sudo docker run -it  --rm --name pTest -p 8900:8900 \
     -v /projects/grapevine/GIT/src/src/main/python/WebServices/DataPublish/CESGA.dockerInfrastructure/python/log:/logs \
     --env NGINX_HOST='https://********.******.***/'	\
     --env NGINX_WEB_SERVICE='prediction'	\
     --env NGINX_PARAMETER='weeksAgo'	\
     --env NGINX_USER='*******'	\
     --env NGINX_PASSWORD='*********'	\
     --env LOG_PATH='log/publish.log'	\
     $IMAGENAME:$VERSION

echo '.'    
echo 'sudo docker exec -it pTest bash'
echo 'sudo docker rm -f pTest'
echo '.'