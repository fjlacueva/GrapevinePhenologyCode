FROM ubuntu:20.04

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ="europe/madrid"

RUN apt update
RUN apt install python3.8 -y

RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.8 1 && update-alternatives --config python3 &&  rm /usr/bin/python3 && ln -s python3.8 /usr/bin/python3
RUN apt update && apt-get upgrade -y
RUN apt install python3.8-dev python3-pip libsqlite3-dev sqlite3 libpq-dev curl -y
RUN apt update && apt-get upgrade -y
RUN apt install wget  -y
RUN apt update && apt-get upgrade -y
RUN apt install python-tk -y
RUN apt install python3-tk -y

RUN mkdir -p /python
RUN mkdir -p /logs
RUN mkdir -p /data
RUN mkdir -p /data/dataPresentation

ADD ./python /python
WORKDIR  /python
RUN pip3 install -r dataPresentationEnvRequeriments.txt
RUN pip3 install Flask

ENV METEOROLOGICAL_DATABASE '********'
ENV METEOROLOGICAL_USER '***********' 
ENV METEOROLOGICAL_PASSWORD '************'
ENV METEOROLOGICAL_HOST '*********.**********.***'
ENV METEOROLOGICAL_PORT *****
ENV TMP_PATH '/tmp/'

ENV OUTPUTCESGAFILENAME 'output_cesga.csv'
ENV OUTPUTCESGAFILENAMEDISEASE 'output_disease_cesga.csv'
ENV OUTPUTCESGAFILENAMEPHENOLOGY 'output_phenology_cesga.csv'
ENV CONFIGOUTPUTPATH '/data/dataPresentation/'
ENV CONFIGOUTPUTPATH_4EMPTYFILES '/data/dataPresentation/emptyFiles'

ENV CHANGECOLUMNNAME {'variedad' : 'variety'}
ENV COPYFILE False

ENV LOGSPATH "/logs"
ENV PYTHONPATHS "/python/Tools"
ENV WORKINGFOLDER "/tmp"
ENV DATAPATH "/data/dataPresentation"
ENV DATAPATH_4EMPTYFILES "/data/dataPresentation/emptyFiles"

ENV LOGLEVEL "DEBUG"

ADD infiniteLoop.sh /tmp/infiniteLoop.sh
RUN chmod 777 /tmp/infiniteLoop.sh

EXPOSE 8800

CMD python3.8 dataPresentation.py 8800