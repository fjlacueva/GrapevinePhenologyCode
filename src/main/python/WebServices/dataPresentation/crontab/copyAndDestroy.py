print('Executing copyAndDestroy.py.importing...')
from ast import Return
from genericpath import isfile
import sys
import os
import configHost as config
import vpn.vpnConnect as vpnManager

def copyAndDestroy2VPN(file2CopyPath):
    print('Executing copyAndDestroy.py...')

    if( not os.path.exists(file2CopyPath)):
        print('{} is not a file!. I Just copy existing files'.format(file2CopyPath))
        sys.exit(+2)
        Return
    if( os.path.isdir(file2CopyPath)):
        print('{} is a dir!. I Just copy files'.format(file2CopyPath))
        sys.exit(+3)
        Return
    encryptedFileWithCommand=config.vpn_scriptFile
    privateKeyPath=config.vpn_privateSVNPEMPathbasePath
    print ('Copying to CESGA {}...({},{},{})'.format(file2CopyPath, encryptedFileWithCommand, privateKeyPath, file2CopyPath))
    x=vpnManager.executeEncryptedScript(encryptedFileWithCommand, privateKeyPath, file2CopyPath)
    sys.exit(0)
    Return
# copyAndDestroy2VPN('/projects/grapevine/GIT/src/src/main/python/WebServices/dataPresentation/crontab/deploy.md')
if len(sys.argv) != 2:
   print('WTF! I do need a file bro!')
   sys.exit(+1)
   Return
else:
    copyAndDestroy2VPN(sys.argv[1])
