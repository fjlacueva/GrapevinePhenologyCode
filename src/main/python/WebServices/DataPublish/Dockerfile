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

ADD ./ /python
WORKDIR  /python
RUN pip3 install -r requeriments.txt

ADD infiniteLoop.sh /tmp/infiniteLoop.sh
RUN chmod 777 /tmp/infiniteLoop.sh

EXPOSE 8900

CMD python3.8 app_publish.py 8900