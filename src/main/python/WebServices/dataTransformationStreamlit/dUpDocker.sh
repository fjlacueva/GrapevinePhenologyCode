#!/bin/bash

cd /pythonEnv/bin
source activate
cd /python
python -m streamlit run Pistara.py --server.address=dataTransformationWSStreamlit --server.port 8501