This web application needs to be run under sudo credentials as it needs to run other docker commands (see /dataSiar, /dataAemet)\

**NOTES**:
- **To run the program on host**: > sudo $PYTHONENV/python appDataRetrieval.py \

- **To activate project environment keeping it active, execute command** > . launchEnv.sh\
Activation is only needed if new libraries are to be installed on the environment.

- **To recreate the environment from the envRequirements.txt file**:
    ```
    python3 -m venv --copies /python/.envs/dataRetrievalEnv #copies avoids symlinks to be used
    . ./launchEnv.sh
    pip install -r envRequirements.txt 
    ```
