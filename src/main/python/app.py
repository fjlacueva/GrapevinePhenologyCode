from flask import request, session,jsonify, Flask
import WebServices.TestGetDataFromUrlToDatabase as testUrl
import sys
from gevent.pywsgi import WSGIServer

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

app = Flask(__name__)

@app.route("/grapevine/getForecast/", methods=['GET'])
def getForecast():

    try:
        ejecucion = testUrl.getForecast()
        if ejecucion == 1:
            message = {"message": ejecucion}
            return jsonify(message)
        else:
            message = {"message": ejecucion}
            return jsonify(message)
    except Exception as e:
        message = {"message": ejecucion}
        return jsonify(message)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
    root = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else ""
    app.wsgi_app = PrefixMiddleware(app.wsgi_app, prefix=root)
    server = WSGIServer(("0.0.0.0", port), app)
    try:
        server.serve_forever()
    except Exception as e:
        exit(-1)