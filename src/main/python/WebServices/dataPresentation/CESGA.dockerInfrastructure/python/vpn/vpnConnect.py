import sys
import os
import shutil
import json, sys, base64, tempfile, subprocess, time
from turtle import pu
import cryptography
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
import config as cfg

# shutil.copyfile(orgPath, dstPath)


#!/usr/bin/env python2

__author__ = "Heewon Lee and Andrea Lazzarotto"
__copyright__ = "Copyright 2014+, Heewon Lee and Andrea Lazzarotto"
__license__ = "GPLv3"
__version__ = "1.0"
__maintainer__ = "Heewon Lee"
__email__ = "rishubil@gmail.com"

OPENVPN_PATH = "openvpn"
VPNGATE_API_URL = "http://www.vpngate.net/api/iphone/"
DEFAULT_COUNTRY = "US"
SELECTED_COUNTRY = ""
DEFAULT_SERVER = 0
YES = False

def getServers():
    servers = []
    server_strings = "requests.get(VPNGATE_API_URL).text"
    for server_string in server_strings.replace("\r", "").split('\n')[2:-2]:
        (HostName, IP, Score, Ping, Speed, CountryLong, CountryShort, NumVpnSessions, Uptime, TotalUsers, TotalTraffic, LogType, Operator, Message, OpenVPN_ConfigData_Base64) = server_string.split(',')
        server = {
            'HostName': HostName,
            'IP': IP,
            'Score': Score,
            'Ping': Ping,
            'Speed': Speed,
            'CountryLong': CountryLong,
            'CountryShort': CountryShort,
            'NumVpnSessions': NumVpnSessions,
            'Uptime': Uptime,
            'TotalUsers': TotalUsers,
            'TotalTraffic': TotalTraffic,
            'LogType': LogType,
            'Operator': Operator,
            'Message': Message,
            'OpenVPN_ConfigData_Base64': OpenVPN_ConfigData_Base64
        }
        servers.append(server)
    return servers

def getCountries(server):
    return set((server['CountryShort'], server['CountryLong']) for server in servers)

def printCountries(countries):
    print("    Connectable countries:")
    newline = False
    for country in countries:
        print("    %-2s) %-25s" % (country[0], country[1])),
        if newline:
            print('\n'),
        newline = not newline
    if newline:
        print('\n'),

def printServers(servers):
    print("  Connectable Servers:")
    for i in xrange(len(servers)):
        server = servers[i]

        ipreq = "requests.get('https://ipinfo.io/%1s' % (server['IP']))"
        ipinfo = json.loads(ipreq.text)

        print("    %2d) %-15s [%6.2f Mbps, ping:%4s ms, score: %3s, hostname: %4s," % (i,
                                                                        server['IP'],
                                                                        float(server['Speed'])/10**6,
                                                                        server['Ping'],
                                                                        server['Score'],
                                                                        ipinfo['hostname']))

        print("                          city: %1s, region: %2s, org: %3s ]\n" % (ipinfo['city'], ipinfo['region'], ipinfo['org'].split(' ', 1)[1]))

def generatePrivateAndPublicKeys(privateKeyPath, publicKeyPath):
    # privateKeyPath: 'private_noshare.pem'
    # publicKeyPath: 'public_shared.pem'
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )
    public_key = private_key.public_key()

    # private key
    serial_private = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    with open(privateKeyPath, 'wb') as f: f.write(serial_private)
        
    # public key
    serial_pub = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )
    with open(publicKeyPath, 'wb') as f: f.write(serial_pub)


#########      Private device only    ##########
def read_private (privateKeyPath):
    with open(privateKeyPath, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )
    return private_key
                  
######### Public (shared) device only ##########
def read_public (publicKeyPath):
    with open(publicKeyPath, "rb") as key_file:
        public_key = serialization.load_pem_public_key(
            key_file.read(),
            backend=default_backend()
        )
    return public_key    

def encrypt(data, publicKeyPath, outputFilePath):
    # data = [b'My secret weight', b'My secret id']
    public_key = read_public(publicKeyPath)
    for encode in data:
        encrypted = public_key.encrypt(
            encode,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256())
                , algorithm=hashes.SHA256()
                , label=None
            )
        )
    print('e.len=' + str(len(encrypted)))
    with open(outputFilePath, "wb") as f: f.write(encrypted)

def decrypt( inputFilePath, privateKeyPath):
    read_data = []
    private_key = read_private(privateKeyPath)
    with open(inputFilePath, "rb") as f:
        for encrypted in f:
            read_data.append(
                private_key.decrypt(
                    encrypted,
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )))
    return read_data 

def doCopy(src, dst):
    # sshpass -p "password" scp @src user@ft2.cesga.es:/mnt/netapp_ext2/Grapevine/cgonzalez

    return 0

def selectCountry(countries):
    selected = SELECTED_COUNTRY
    default_country = DEFAULT_COUNTRY
    short_countries = list(country[0] for country in countries)
    if not default_country in short_countries:
        default_country = short_countries[0]
    if YES:
        selected = default_country
    while not selected:
        try:
            selected = raw_input("[?] Select server's country to connect [%s]: " % (default_country, )).strip().upper()
        except:
            print("[!] Please enter short name of the country.")
            selected = ""
        if selected == "":
            selected = default_country
        elif not selected in short_countries:
            print("[!] Please enter short name of the country.")
            selected = ""
    return selected

def selectServer(servers):
    selected = -1
    default_server = DEFAULT_SERVER
    if YES:
        selected = default_server
    while selected == -1:
        try:
            selected = raw_input("[?] Select server's number to connect [%d]: " % (default_server, )).strip()
        except:
            print("[!] Please enter vaild server's number.")
            selected = -1
        if selected == "":
            selected = default_server
        elif not selected.isdigit() or int(selected) >= len(servers):
            print("[!] Please enter vaild server's number.")
            selected = -1
    return servers[int(selected)]

def saveOvpn(server):
    _, ovpn_path = tempfile.mkstemp()
    ovpn = open(ovpn_path, 'w')
    ovpn.write(base64.b64decode(server["OpenVPN_ConfigData_Base64"]))
    ovpn.write('\nscript-security 2\nup /etc/openvpn/update-resolv-conf\ndown /etc/openvpn/update-resolv-conf'.encode())
    ovpn.close()
    return ovpn_path

def connect(ovpn_path):
    openvpn_process = subprocess.Popen(['sudo', OPENVPN_PATH, '--config', ovpn_path])
    try:
        while True:
            time.sleep(600)
    # termination with Ctrl+C
    except:
        try:
            openvpn_process.kill()
        except:
            pass
        while openvpn_process.poll() != 0:
            time.sleep(1)
        print("[=] Disconnected OpenVPN.")

    # https://towardsdatascience.com/asymmetric-encrypting-of-sensitive-data-in-memory-python-e20fdebc521c
    # publicKeyPath=cfg.basePath + 'certificates/vpn/private/publicVpn.pem'
    # privateKeyPath=cfg.basePath + 'certificates/vpn/2Share/privateVpn.pem'
    # generatePrivateAndPublicKeys(privateKeyPath, publicKeyPath)
def encryptValueInfoFile(value, publicKeyPath, outputFilePath):
    # value=b'HERE should be the command to be encrypted. E.g.sshpass -p "PASSWORD" scp {} USER@ft2.cesga.es:/mnt/netapp_ext2/Grapevine/...'
    # publicKeyPath=cfg.basePath + 'certificates/vpn/private/publicVpn.pem'
    # outputFilePath=cfg.basePath + 'python/webServers/vpn/vpnConfig2.json'
    encrypt([value], publicKeyPath, outputFilePath)

def executeEncryptedScript( encryptedFileWithCommand, privateKeyPath, *args):
    # encryptedFileWithCommand: Contains the file with the command to be executed and encrypted using the encryptValueInfoFile method
    # privateKeyPath=cfg.basePath + 'certificates/vpn/2Share/publicVpn.pem'
    # publicKeyPath=cfg.basePath + 'certificates/vpn/private/privateVpn.pem'
    # value=[b'sshpass -p "xxx" scp {} uuu@remoteServer:remoteFolder']
    # generatePrivateAndPublicKeys(privateKeyPath, publicKeyPath)

    # encrypt(value,publicKeyPath, encryptedFileWithCommand)
    # args: file2CopyPath= cfg.basePath + 'data/csv_salida220216.csv'
    scriptCommand= decrypt(encryptedFileWithCommand, privateKeyPath)[0].decode('ascii')
    # print('scriptCommand:'+scriptCommand.format(*args))
    os.system(scriptCommand.format(*args))
    ####
# executeEncryptedScript('/projects/grapevine/GIT/src/src/main/python/WebServices/dataPresentation/vpn/vpnConfig2.json',
# '/projects/grapevine/GIT/src/src/main/python/WebServices/dataPresentation/certificates/vpn/2Share/privateVpn.pem',
# '/projects/grapevine/GIT/src/data/DownloadFiles.log')
# exit()
if __name__ == "__main__":
    # encryptedFileWithCommand=cfg.basePath + 'python/webServers/vpn/vpnConfig2.json'
    # privateKeyPath=cfg.basePath + 'certificates/vpn/2Share/privateVpn.pem'
    # file2CopyPath= cfg.basePath + 'data/csv_salida220216.csv'
    # executeEncryptedScript(encryptedFileWithCommand, privateKeyPath, file2CopyPath)
    if len(sys.argv) > 1:
        if sys.argv[1] == "-y":
            YES = True
        else:
            SELECTED_COUNTRY = sys.argv[1]

    servers = []
    try:
        print("[-] Trying to get server's informations...")
        servers = sorted(getServers(), key=lambda server: int(server["Score"]), reverse=True)
    except:
        print("[!] Failed to get server's informations from vpngate.")
        sys.exit(1)

    if not servers:
        print("[!] There is no running server on vpngate.")
        sys.exit(1)

    print("[-] Got server's informations.")

    countries = sorted(getCountries(servers))

    if not SELECTED_COUNTRY:
        printCountries(countries)

    selected_country = selectCountry(countries)

    print("[-] Gethering %s servers..." % (selected_country, ))

    selected_servers = [server for server in servers if server['CountryShort'] == selected_country]
    printServers(selected_servers)
    selected_server = selectServer(selected_servers)

    print("[-] Generating .ovpn file of %s..." % (selected_server["IP"], ))

    ovpn_path = saveOvpn(selected_server)

    print("[-] Connecting to %s..." % (selected_server["IP"], ))

    connect(ovpn_path)
