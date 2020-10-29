#hopefully Python is magical and figures out that MySQL is supposed to be an object.
#
#This object exists to represent a MySQL or MariaDB connection (really, anything compatible with mysql-connector).
#It holds a pandas dataframe for temporary data storage, along with methods for uploading and downloading data, as well as creating and preparing tables for such.
#The only initially required parameter is a path to the .yaml with database credentials.

class MySQL:
    #init- we go ahead and import credentials here, and specify target SQL table regardless of whether it yet exists.
    def __init__(self,credPath,dbName,dbTable):
        #where this is intended to be used coming and going, dat will be initialized empty. Populate it via 'objName.dat=<dataframe>'.
        import pandas as pd
        self.dat = pd.DataFrame()
        #this is a little ugly, but we don't want a password hanging out in the object, so it's better to go ahead and make the connection to pass around instead.
        #In the future, I would this to utilize parallel connections like Informatica does, but I'm unsure if Python can do so

        self.filePath = credPath

        #self.cnx = self.importDbCredentials(credPath)
        self.cnx = ''

        #the table we will load to or from:
        self.table = dbTable

        #the database we will load to or from:
        self.db = dbName

        #needed to see if SQL table exists:
        #self.SQL_exists = self.checkForSQL()
        self.SQL_exists = False

        #run startup tasks:
        self.importDbCredentials()
        self.checkForSQL()

    #objectively, guessing good column types will be difficult
    #best we can do is pandas.dtypes
    def createSQLTable(self):
        import numpy as np
        #read column dtypes to start:
        cols=self.dat.dtypes.to_dict()
        for col in cols:
            print(col)
            if cols[col]==np.dtype('O'):
                #print('derp')
                cols[col]='varchar'
            if cols[col]==np.dtype('<M8[ns]'):
                #print('derp')
                cols[col]='datetime'
            if cols[col]==np.dtype('int64') or cols[col]==np.dtype('uint64'):
                #print('derp')
                cols[col]='bigint'
            if cols[col]==np.dtype('int32') or cols[col]==np.dtype('uint32'):
                #print('derp')
                cols[col]='bigint'
            if cols[col]==np.dtype('int16') or cols[col]==np.dtype('uint16'):
                #print('derp')
                cols[col]='mediumint'
            if cols[col]==np.dtype('int8') or cols[col]==np.dtype('uint8'):
                #print('derp')
                cols[col]='mediumint'
            if cols[col]==np.dtype('int_'):
                #print('derp')
                cols[col]='mediumint'
            if cols[col]==np.dtype('bool_'):
                #print('derp')
                cols[col]='boolean'
            if cols[col]==np.dtype('float_'):
                #print('derp')
                cols[col]='double'
            if cols[col]==np.dtype('float16'):
                #print('derp')
                cols[col]='double'
            if cols[col]==np.dtype('float32'):
                #print('derp')
                cols[col]='double'
            if cols[col]==np.dtype('float64'):
                #print('derp')
                cols[col]='double'
        print(cols)

    #uploads the file to specified db and table, mildly cleaning to avoid accidental SQL injection. If 'truncate=True', it will truncate.
    def uploadFile(self,truncate=False):
        #Fix strings so they don't mess up SQL commands
        def fixCell(strVal):
            strVal=strVal.replace("'", "''")
            strVal=strVal.replace("\n", " ")
            return(strVal)
        def toStr(val):
            val=str(val)
            return(val)
        #make cursor:
        cursor=cnx.cursor()
        #connect to database:
        try:
            cursor.execute("USE {};".format(self.db))
        except:
            print("Database not found")
            raise
        #clean out potential escape characters, others that could cause SQL issues
        dat=dat.where((pd.notnull(dat)), None)
        dat.apply(fixCell)
        dat.apply(toStr)
        dat=dat.values.tolist()
        #truncate before loading (if specified):
        if truncate==True:
            cursor.execute('truncate table '+self.db+'.'+self.table+';')
        #get columns from target table:
        cursor.execute('show columns from '+self.db+'.'+self.table+';')
        cols=cursor.fetchall()
        #draft template insert statement:
        query=''
        query2=''
        count=0
        for i in cols:
            count=count+1
            col='`'+i[0]+'`'
            if count==len(cols): #to avoid comma issues
                query=query+col
                query2=query2+'%s'
            else:
                query=query+col+', '
                query2=query2+'%s, '
        query='INSERT INTO '+self.db+'.'+self.table+'('+query+')'+' VALUES ('+query2+')'
        print(query)
        print(dat[1])
        cursor.executemany(query, dat)
        cnx.commit()
        cursor.close()

    #checkForSQL: tests whether SQL table exists.
    def checkForSQL(self):
        #make cursor:
        cursor=self.cnx.cursor()
        #connect to database:
        try:
            cursor.execute("USE {};".format(self.db))
        except:
            print("Database not found")
            raise
        #see tables in specified database:
        cursor.execute('show tables;')
        cols=cursor.fetchall()
        for col in cols:
            if col[0]==self.table:
                self.SQL_exists = True
        cursor.close()

    #import credentials for mysql-connector object, validates, connects, returns mysql-connector connection object
    #these should be a .yaml with the following:
    #
    #hostname='foo.bar'
    #username='foo'
    #password='bar'
    #
    #Make sure the config is properly secured on your machine.
    def importDbCredentials(self):
        import yaml
        import mysql.connector
        try:
            with open(self.filePath,'r') as file:
                loginInfo = yaml.full_load(file)
        #I like to provide more user-friendly error messages when possible:
        except FileNotFoundError:
            print('File not found at '+self.filePath)
            raise FileNotFoundError('credential file at '+self.filePath)
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
        #try connection:
        try:
            self.cnx=mysql.connector.connect(user=loginInfo['username']
                                    , password=loginInfo['password']
                                    , host=loginInfo['hostname'])
        except mysql.connector.errors.InterfaceError: #2003:
            print('Cannot login: host not found')
            raise
        except mysql.connector.errors.ProgrammingError: #1045 (28000):
            print('Cannot login: access denied (likely an incorrect username or password)')
            raise
        except:
            print('Cannot login: unknown error occured while connection to database.')
            raise
            #if all went well:
            return True

    #Will import all columns. Column-dropping will occur outside this object
    def downloadFile(self):
        import pandas as pd
        #first, raise an error if target table doesn't exit:
        #refresh SQL table status first
        self.checkForSQL()
        if not self.SQL_exists:
            print('target table doesnt exist. Nothing to import.')
            raise RuntimeError('target table doesnt exist')
        #make cursor:
        cursor=self.cnx.cursor()
        #fetch data
        cursor.execute('select * from '+self.db+'.'+self.table+';')
        self.dat=pd.DataFrame(cursor.fetchall())
        #make headers meaningful:
        #get column names from SQL
        cursor.execute('show columns from '+self.db+'.'+self.table+';')
        cols=cursor.fetchall()
        #reformat them in a sane way:
        colNames=[]
        for col in cols:
            colNames.append(col[0])
        self.dat.columns=colNames
