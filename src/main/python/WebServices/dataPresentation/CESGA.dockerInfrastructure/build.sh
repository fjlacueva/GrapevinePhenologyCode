#!/bin/bash
sudo docker rm -f pTest

mkdir -p /projects/grapevine/GIT/src/src/main/python/WebServices/dataPresentation/CESGA.dockerInfrastructure/python

PYTHONPATH=/projects/grapevine/GIT/src/src/main/python/WebServices/dataPresentation/
DOCKERBUILDPATH=$PYTHONPATH/CESGA.dockerInfrastructure
rsync -av --exclude=__pycache__ --exclude=certificates --exclude=CESGA  --exclude=CESGAKubernetes --exclude=crontab --exclude=docker --exclude=nginx $PYTHONPATH $DOCKERBUILDPATH/python

PYTHONPATH=/projects/grapevine/GIT/src/src/main/python/Tools
rsync -av --exclude=__pycache__ $PYTHONPATH $DOCKERBUILDPATH/python

IMAGENAME=datapresentationmanager
VERSION=1.0.2

cd $DOCKERBUILDPATH
docker build -t $IMAGENAME:latest -t $IMAGENAME:$VERSION .

echo 'FOR TESTING ON DOCKER:'
echo sudo docker run -it  --rm --name pTest -p 8800:8800 \
     -v /projects/grapevine/GIT/src/data/dataPresentation:/data/dataPresentation/       \
     -v /projects/grapevine/GIT/src/logs:/logs \
     --env METEOROLOGICAL_DATABASE='*****'	\
     --env METEOROLOGICAL_USER='********'	\
     --env METEOROLOGICAL_PASSWORD='********'	\
     --env METEOROLOGICAL_HOST='********.********.****'	\
     --env METEOROLOGICAL_PORT=****	\
     --env TMP_PATH='/tmp/'	\
     --env LOGLEVEL='DEBUG'	\
     --env OUTPUTCESGAFILENAME='output_cesga.csv'	\
     --env OUTPUTCESGAFILENAMEDISEASE='output_disease_cesga.csv'	\
     --env OUTPUTCESGAFILENAMEPHENOLOGY='output_phenology_cesga.csv'	\
     --env CONFIGOUTPUTPATH='/data/dataPresentation/'	\
     --env CONFIGOUTPUTPATH_4EMPTYFILES='/data/dataPresentation/'	\
     --env COPYFILE='False'	\
     $IMAGENAME:$VERSION

echo '.'    
echo 'sudo docker exec -it pTest bash'
echo 'sudo docker rm -f pTest'
echo '.'