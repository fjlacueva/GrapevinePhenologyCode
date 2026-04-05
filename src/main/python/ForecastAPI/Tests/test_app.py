import os, sys
import json
from flask import Flask, request, Response, json

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir) # Move one folder up
sys.path.append(parentdir)
from app import app

def test_getForecast():

    client = app.test_client()
    url = '/grapevine/getForecast/'
    response = client.get(url)

    print('RESPONSE:::', response.json())

    assert response.status_code == 200