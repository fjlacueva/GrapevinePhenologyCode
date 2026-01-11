#!/bin/bash

# This script is initially a copy of the existing on the /argon/src/main/script/siar-download-insert.sh
# But instead of invoking a docker image, it knows that code is into the /tmp/descsiar.py

function load_severals_days(){
    for i in {17..1}
    do
        echo docker run --rm -it  --name=sarga-siar --log-driver syslog --log-opt tag="{{.ImageName}}/{{.Name}}"  --network=argon_backend --env LOGLEVEL="INFO" argon-aemet  sh -c " date '+%Y%m%d'   -d '$i day ago' | xargs  python descsiar.py -d  "
    done
}

function load_one_and_previous_day(){
    logger cron sarga-siar insert
    dia=$(date '+%Y%m%d' -d '1 day ago')
    $PYTHONENV/bin/python $WORKINGFOLDER/descsiar.py -d $dia 
    logger cron sarga-siar end-insert
}

export LOGLEVEL="INFO"
load_one_and_previous_day
#load_severals_days
