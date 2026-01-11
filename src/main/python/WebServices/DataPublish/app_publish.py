import requests
import sys

from flask import Flask, jsonify, request
from flask_cors import CORS
from gevent.pywsgi import WSGIServer
import config as cf
from requests.auth import HTTPBasicAuth
import urllib3

import log.log as l

class PrefixMiddleware(object):
    def __init__(self, app, prefix=""):
        self.app = app
        self.prefix = prefix

    def __call__(self, environ, start_response):
        if environ["PATH_INFO"].startswith(self.prefix):
            environ["PATH_INFO"] = environ["PATH_INFO"][len(self.prefix) :]
            environ["SCRIPT_NAME"] = self.prefix
            return self.app(environ, start_response)
        else:
            start_response("404", [("Content-Type", "text/plain")])
            return ["This url does not belong to the app.".encode()]

app_predict = Flask(__name__)
CORS(app_predict)
logger = l.configLog(cf.LOG_PATH)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

basicAuth = HTTPBasicAuth(username=cf.NGINX_USER, password=cf.NGINX_PASSWORD)

@app_predict.route("/",methods = ['GET'])
def root():
    return "<p>Welcome to the Public Web Service</p>"

@app_predict.route(cf.vineyardsDiseasePrediction_endPoint, methods = ['GET'])
def training():
    """
        Call to the prediction process service.
        ---
        tags:
            - app_predict
    """

    weeksAgo = int(request.args.get('weeksAgo')) if 'weeksAgo' in request.args else 0;
    logger.info("Predict porcess has started")
    try:
        url = cf.NGINX_HOST + cf.NGINX_WEB_SERVICE + '?' + cf.NGINX_PARAMETER + '=' + str(weeksAgo)
        predict = requests.get(url, auth = basicAuth, verify=False, timeout=None)
        logger.info("Predict porcess has finished correctly")
        return jsonify({"error": False, "message": predict.json()})
    except Exception as e:
        logger.info("Predict porcess has finished incorrectly")
        return jsonify({"error": True, "message": "The prediction exection process has finished incorrectly. " + str(e)})

if __name__ == "__main__":
     port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
     root = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else ""
     app_predict.wsgi_app = PrefixMiddleware(app_predict.wsgi_app, prefix=root)
     server = WSGIServer(("0.0.0.0", port), app_predict)
     try:
         server.serve_forever()
     except Exception as e:
         exit(-1)
