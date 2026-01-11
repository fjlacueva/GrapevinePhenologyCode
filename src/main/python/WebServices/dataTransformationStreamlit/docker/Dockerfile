# app/Dockerfile

FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir /dataTransformationStreamlit/

COPY src/src/main/python/WebServices/dataTransformationStreamlit /dataTransformationStreamlit/

WORKDIR /dataTransformationStreamlit/

RUN pip3 install -r requirements.txt

RUN mkdir /python/
RUN mkdir /python/Tools

COPY /projects/grapevine/GIT/src/src/main/python/pistara /python/
COPY /projects/grapevine/GIT/src/src/main/python/Tools /python/Tools

WORKDIR /python/

ENTRYPOINT ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=dataTransformationWSStreamlit"]