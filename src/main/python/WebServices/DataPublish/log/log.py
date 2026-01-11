import logging

def configLog(path):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(path)
    fh.setLevel(logging.DEBUG)
    ff = logging.Formatter('%(asctime)s %(name)s %(levelname)s::: %(message)s')
    fh.setFormatter(ff)
    logger.addHandler(fh)
    return logger