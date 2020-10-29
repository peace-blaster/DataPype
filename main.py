#Julian DeVille
# 20200622 (quickly hacked into current project 20201018)
# main.py- calls needed functions to clean and load the provided 'datafile.csv' (will be hardcoded)

#to install modules we may need from inside the script:
def install(name):
    subprocess.call(['pip3', 'install', name])
#import our other scripts (in practice, this will make version management and debugging easier)
from MySQL.cleanData import cleanData
from MySQL.addUuid import addUuid
from MySQL.uploadFile import uploadFile
from MySQL.uploadFile import importDbCredentials
#make sure we have needed modules
import sys
import subprocess
#Pandas: needed for nice data structure, efficient apply() function
try:
    import pandas as pd
except:
    print("Pandas module not found: installing...")
    try:
        if raw_input('install missing module \'pandas\' now? (y/n)')=='y':
            install('pandas')
        else:
            print('Quitting')
            quit()
    except:
        print('Something went wrong installing pandas.')
        print('Quitting')
        quit()
    import pandas as pd
#MySQL-Connector: needed to connect
try:
    import mysql.connector
except:
    print("mysql-connector module not found: installing...")
    try:
        if raw_input('install missing module \'mysql-connector\' now? (y/n)')=='y':
            install('mysql-connector')
        else:
            print('Quitting')
            quit()
    except:
        print('Something went wrong installing mysql-connector.')
        print('Quitting')
        quit()
    import mysql.connector
#uuid: needed for normalization later
try:
    import uuid
except:
    print("uuid module not found: installing...")
    try:
        if raw_input('install missing module \'uuid\' now? (y/n)')=='y':
            install('uuid')
        else:
            print('Quitting')
            quit()
    except:
        print('Something went wrong installing uuid.')
        print('Quitting')
        quit()
    import uuid
#dateutil: needed for cleaning
try:
    import dateutil
except:
    print("dateutil module not found: installing...")
    try:
        if raw_input('install missing module \'dateutil\' now? (y/n)')=='y':
            install('dateutil')
        else:
            print('Quitting')
            quit()
    except:
        print('Something went wrong installing dateutil.')
        print('Quitting')
        quit()
    import dateutil
#dateutil: needed for cleaning
try:
    import yaml
except:
    print("pyyaml module not found: installing...")
    try:
        if raw_input('install missing module \'pyyaml\' now? (y/n)')=='y':
            install('pyyaml')
        else:
            print('Quitting')
            quit()
    except:
        print('Something went wrong installing pyyaml.')
        print('Quitting')
        quit()
    import yaml
#there is no "main" here really, these are just examples:

#import credentials:
cred=importDbCredentials('/home/peaceblaster/Pyformatica/mariadbLogin.yaml')
#import the data file: note that addUuid() will add the UUID column automatically
dat=pd.read_csv(filePath,sep='|')
#clean up the file:
dat=cleanData(dat)
#add UUIDs: YOUR TARGET TABLE MUST HAVE COLUMN CALLED 'UUID'!!
dat=addUuid(dat)
#show me:
print(dat.head())
#upload cleaned data
uploadFile(dat, hostname, user, pw, db, tbl)
