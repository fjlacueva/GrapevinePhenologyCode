import logging
import main.python.ForecastAPI.config as cf

def configLog(path):

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(path)
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)
    return logger