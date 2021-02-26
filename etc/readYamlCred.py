import yaml
#import credentials for mysql-connector object, validates, connects, returns mysql-connector connection object
#these should be a .yaml with the following:
#
#hostname: 'foo.bar'
#username: 'foo'
#password: 'bar'
#
#Make sure the config is properly secured on your machine.
def readYamlCred(filePath):
    try:
        with open(filePath,'r') as file:
            loginInfo = yaml.full_load(file)
    #I like to provide more user-friendly error messages when possible:
    except FileNotFoundError:
        print('File not found at '+filePath)
        raise FileNotFoundError('credential file at '+filePath)
    #ensure credentials file is valid:
    #first see if it's a .yaml at all, and that it was properly read:
    if not type(loginInfo) is dict:
        print('Cannot login: Provided file is incorrectly formatted, and could not be read.')
        raise RuntimeError('bad credential file')
    #trim unneeded fields:
    badFields=[]
    for field in loginInfo:
        if field not in ['hostname','username','password']:
            badFields.append(field)
    for field in badFields:
        loginInfo.pop(field)
    #ensure all needed fields are present:
    #hostname
    try:
        loginInfo['hostname']
    except KeyError:
        print('Cannot login: hostname not provided in credential file.') #why isn't this printing?
        raise
    #username
    try:
        loginInfo['username']
    except KeyError:
        print('Cannot login: username not provided in credential file.')
        raise
    #password
    try:
        loginInfo['password']
    except KeyError:
        print('Cannot login: password not provided in credential file.')
        raise
    return loginInfo #returns a dict
