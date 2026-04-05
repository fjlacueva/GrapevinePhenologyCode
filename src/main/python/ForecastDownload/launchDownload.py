import DownloadFiles as d
import config as cf
import json
import sys

# logging.basicConfig( level=logging.DEBUG, force=True)

def loadDataAgroappsData(dateFromStr = None,endDateStr = None,resolution = None):

    if not sys.argv is None and len(sys.argv)> 1:
        if len(sys.argv)>=3:
            for expectedArgv in cf.expectedParameters:
                if expectedArgv in sys.argv  :
                    if expectedArgv == "startdate" :
                        dateFromStr = sys.argv[sys.argv.index(expectedArgv)+1]
                    if expectedArgv == "endate":
                        endDateStr = sys.argv[sys.argv.index(expectedArgv)+1]
                    if expectedArgv == "resolution":
                        resolution = sys.argv[sys.argv.index(expectedArgv)+1]
    if resolution is None or len(resolution) == 0:
        resolutions = ["18km", "6km"]
    else:
        resolutions = [resolution]
    for resolution in resolutions:
        loaded = d.downloadFromDate(dateFromStr=dateFromStr, endDateStr=endDateStr, resolution=resolution )

    # valuesString = json.dumps(loaded, indent=4, sort_keys=True, cls=d.ObjectEncoder )
    # print(valuesString)

# loadDataAgroappsData('20220514','20220515',"18km")