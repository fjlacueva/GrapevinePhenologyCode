import os
import sys

paths2Libraries=['/projects/grapevine/GIT/src/src/main/python/WebServices/dataPresentation']
if paths2Libraries != None:
    for path2Library in paths2Libraries:
        if os.path.isdir(path2Library) and path2Library not in sys.path:
          sys.path.append(path2Library)
# print('Path:{}'.format(sys.path))

# VPN Config
############
basePath='/projects/grapevine/GIT/src/src/main/python/WebServices/dataPresentation'
vpn_privateSVNPEMPathbasePath= basePath + '/certificates/vpn/2Share/privateVpn.pem'
vpn_scriptFile=basePath + '/vpn/vpnConfig2.json'
