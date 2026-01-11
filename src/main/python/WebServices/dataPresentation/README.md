**NOTES**:
- **To run the program on host**: > $PYTHONENV/python appDiseasesPredictionGenerator.py \

- **To activate project environment keeping it active, execute command** > . launchEnv.sh\
Activation is only needed if new libraries are to be installed on the environment.

- **To recreate the environment from the envRequirements.txt file**:

sudo apt install virtualenv
virtualenv  -p /usr/bin/python3 --copies /python/.envs/dataPresentationEnv
source /python/.envs/dataPresentationEnv/bin/activate
pip install -r requirements.txt

    ```
    rm -rf /python/.envs/dataPresentationEnv
    sudo apt-get install python3.8-dev python3.8-venv
    python3 -m venv --copies /python/.envs/dataPresentationEnv #copies avoids symlinks to be used
    . ./launchEnv.sh
    python -m pip install -U pip
    /python/.envs/dataPresentationEnv/bin/python3 -m pip install --upgrade pip
    pip install -r envRequirements.txt 
    mkdir /data/dataPresentation -p
    ```

**Grapevine components to expose endpoints for the execution of the different predictions scope of the project**  
NOTE: This infrastructure contains the components to be exposed on the grapevine.*******.*** host. The IT has to expose the 80 for the certificate management and 443 ports for the endpoints.

The initial idea was to expose endpoints to generate the models and the generation of the predictions as CSV files.  
Finally on a meeting on December the 14th, it was agreed that only the predictions will be exposed and the generation was going to be executed once on the ITAInnova GPU Servers.  
Currently on December the 15th the prediction endpoints will be two:  

1- The Generation of the predictions for the near future (eg. one week) will no require any parameter.  
>This will be an asynchronous invocation that will launch the process and will return a future description of the process with an id that will help in the future requests to see the status of the tasks.  
This endpoint will be singleton as only one prediction process will be in execution. Invocations of new predictions will return the description of the future already in execution.  
The outcome of this process will be stored in a DB that will later be used to retrieve the predictions (see 2nd endpoint)  

2- The retrieval of the prediction for a specific plot of land.  
>It will require the location of the plot and the predictions to be retrieved (phenology, mildium, ...)  
The process will retrieve from the DB the previously computed predictions.  


**NOTE 220525**:

Currently the infrastructure has evolved and now it is composed by the following docker components:
- **certbot**: for letsencrypt certificate management (renewal)
- **nginx**: for grapevine.*******.*** endpoints configuration. A Basic user/password access policy is set with the following users
    + grapevine.
    
    The following endpoints are exposed:
    >**/health**: To check the docker health
    
        >**/vineyardsPhenologyPrediction**
    >> https://grapevine.*******.***/vineyardsPhenologyPrediction
    ```
        {
            "createdTime": "Thu, 26 May 2022 11:07:10 GMT",
            "done": false,
            "durationSgs": 0.001043,
            "finishedTime": null,
            "id": "vineyardsPhenologyPrediction-fcf85e20-dce3-11ec-ae76-0242ac190002",
            "result": "",
            "status": "RUNNING"
        }
    ```

    >**/diseasePrediction**
    >> https://grapevine.*******.***/vineyardsDiseasePrediction
    ```
        {
            "createdTime": "Thu, 26 May 2022 11:07:15 GMT",
            "done": false,
            "durationSgs": 0.000775,
            "finishedTime": null,
            "id": "vineyardsDiseasePrediction-003b4b1a-dce4-11ec-ae76-0242ac190002",
            "result": "",
            "status": "RUNNING"
        }
    ```

    > **/jobs**: Returns an array with the status of all launched jobs (jobs already finished and seen are removed after the number of seconds defined in the config param future_timeAliveSgs)  
    >> https://grapevine.*******.***/jobs

    ```
            [
                {
                createdTime: "Wed, 15 Dec 2021 10:36:25 GMT",
                done: false,
                durationSgs: 144.010852,
                finishedTime: null,
                id: "phenology-c3f6c888-5e52-11ec-9430-0242c0a8f002",
                result: "",
                status: "RUNNING"
                }
            ]
    ```

    > **​/job​/{id}** Gets the status of the job with the given <id>  
    >> https://grapevine.*******.***/job/phenology-c3f6c888-5e52-11ec-9430-0242c0a8f002
    ```
            {
                createdTime: "Wed, 15 Dec 2021 10:36:25 GMT",
                done: false,
                durationSgs: 144.010852,
                finishedTime: null,
                id: "phenology-c3f6c888-5e52-11ec-9430-0242c0a8f002",
                result: "",
                status: "RUNNING"
            }
    ```


- *predictionWS*: code to launch the different disease predictions and the management of its upload to CESGA SERVERS.

    This server exposes the following endpoints
    > /swagger. Swagger is exposed at the Flask level for internal development purposes. To expose it on NGinx, several locations have to be registered that make no sense. For example, if the Flask of Phenology is exposed on port 8000, the URL should be  
    >>http://127.0.0.1:8000/swagger/

    >**/vineyardsPhenologyPrediction**
    >> https://grapevine.*******.***/vineyardsPhenologyPrediction
    ```
        {
            "createdTime": "Thu, 26 May 2022 11:07:10 GMT",
            "done": false,
            "durationSgs": 0.001043,
            "finishedTime": null,
            "id": "vineyardsPhenologyPrediction-fcf85e20-dce3-11ec-ae76-0242ac190002",
            "result": "",
            "status": "RUNNING"
        }
    ```

    >**/vineyardsDiseasePrediction**
    >> https://grapevine.*******.***/vineyardsDiseasePrediction
    ```
        {
        createdTime: "Thu, 26 May 2022 13:50:11 GMT",
        done: true,
        durationSgs: 53.102809,
        finishedTime: "Thu, 26 May 2022 13:51:04 GMT",
        id: "vineyardsDiseasePrediction-c325ae16-dcfa-11ec-bc0d-0242c0a81002",
        result: "/data/outputs/20220526_135011_vineyardsDiseasePredict.csv",
        status: "FINISHED"
        }
    ```


- There is also a **CRONTAB** process to support the connection to CESGA SERVERS via 'VPN' for the generated files upload to its server
The executed command copyAndDestroy.sh needs the /projects/grapevine/GIT/src/final/python/webServers/copyAndDestroy.py python file to run.
Please, do not move the python from its location as it needs 3th party libraries.
Using crontab -e for user cgonzalez, the command reads as follows
    ```
    * * * * * /projects/grapevine/GIT/src/src/main/python/WebServices/dataPresentation/crontab/copyAndDestroy.sh >> /logs/logCrontabCopyAndDestroy.log 2>&1 
    ``` 


**NOTE 211215** ** DEPRECATED **:

Currently on December the 15th the endpoints available are:
> /swagger. Swagger is exposed at the Flask level for internal development purposes. To expose it on NGinx, several locations have to be registered that make no sense. For example, if the Flask of Phenology is exposed on port 8000, the URL should be  
>>http://127.0.0.1:8000/swagger/

> /phenology/ *TO Be DELETED* Launches the training of the phenology model and returns the information related with the launched asynchronous job  
>> https://grapevine.*******.***/phenology?inputCsv=/data/phenology_vid_procesado.csv&outputCsv=/data/csv_salida.csv
```
        {
            createdTime: "Wed, 15 Dec 2021 10:36:25 GMT",
            done: false,
            durationSgs: 0.00262,
            finishedTime: null,
            id: "phenology-c3f6c888-5e52-11ec-9430-0242c0a8f002",
            result: "",
            status: "RUNNING"
        }
```



t9fTJvkxn5dTmsMcyDzMrmZWL
