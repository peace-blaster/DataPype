#hopefully Python is magical and figures out that MySQL is supposed to be an object.
#
#This object exists to represent a MySQL or MariaDB connection (really, anything compatible with mysql-connector).
#It holds a pandas dataframe for temporary data storage, along with methods for uploading and downloading data, as well as creating and preparing tables for such.

class MySQL:
    #init- we go ahead and import credentials here, and specify target SQL table regardless of whether it yet exists.
    def __init__(self,credPath,dbName,dbTable,importData=[]):
        #where this is intended to be used coming and going, dat will be initialized empty. Populate it via 'objName.dat=<dataframe>' (if worried about memory usage, instead pass in data through the 'importdata' parameter on creation).
        #it will expect meaningful, and SQL-compliant column names. The automatic cleaning is limited at best.
        import pandas as pd

        self.dat = importdata #this will be updated by initial functions

        #this is a little ugly, but we don't want a password hanging out in the object, so it's better to go ahead and make the connection to pass around instead.
        #initially, this will take the path to credentials, and then makeConnection() in the startup tasks.
        #In the future, I would this to utilize parallel connections like Informatica does, but I'm unsure if Python can do so
        self.filePath = credPath

        #self.cnx = self.makeConnection(credPath)
        self.cnx = ''

        #the table we will load to or from:
        #note you can change this later if you want to upload back to the same host
        self.table = dbTable

        #the database we will load to or from:
        #note you can change this later if you want to upload back to the same host
        self.db = dbName

        #needed to see if SQL table exists:
        #self.SQL_exists = self.checkForSQL()
        self.SQL_exists = False

        #to store SQL column types when going pandas --> SQL. Pandas does a good enough job when going SQL --> pandas.
        #if you change column names and run makeSQLColumnTypes() without first reinitializing this, it will append all the new columns and you'll get an error.
        self.SQLColTypes=dict()

        #run startup tasks:
        self.makeConnection()
        self.checkForSQL()
        #if no input provided, make it a dataframe
        if self.dat.isempty:
            self.dat = pd.DataFrame()
            

##################################################################################
##################################################################################
##################################################################################

#createSQLTable():

    #objectively, guessing good column types will be difficult
    #best we can do is be a little generous going off of pandas.dtypes
    #it would be possible to use MySQL's built-in optimizer to alter table after creation for optimal types, but it tends to overuse enum, and is too aggressive to be future-proof.
    def makeSQLColumnTypes(self):
        import numpy as np
        #read column dtypes to start:
        cols = self.dat.dtypes.to_dict()
        for col in cols:
            if cols[col] == np.dtype('O'):
                #was getting 'TypeError: data type 'its a datetime' not understood' when overwriting dtype values, making new dict seems to fix
                self.SQLColTypes[col] = 'varchar'
            if cols[col] == np.dtype('<M8[ns]'):
                self.SQLColTypes[col] = 'datetime'
            if cols[col] == np.dtype('int64') or cols[col] == np.dtype('uint64'):
                self.SQLColTypes[col] = 'bigint'
            if cols[col] == np.dtype('int32') or cols[col] == np.dtype('uint32'):
                self.SQLColTypes[col] = 'bigint'
            if cols[col] == np.dtype('int16') or cols[col] == np.dtype('uint16'):
                self.SQLColTypes[col] = 'mediumint'
            if cols[col] == np.dtype('int8') or cols[col] == np.dtype('uint8'):
                self.SQLColTypes[col] = 'mediumint'
            if cols[col] == np.dtype('int_'):
                self.SQLColTypes[col] = 'mediumint'
            if cols[col] == np.dtype('bool_'):
                self.SQLColTypes[col] = 'boolean'
            if cols[col] == np.dtype('float_'):
                self.SQLColTypes[col] = 'double'
            if cols[col] == np.dtype('float16'):
                self.SQLColTypes[col] = 'double'
            if cols[col] == np.dtype('float32'):
                self.SQLColTypes[col] = 'double'
            if cols[col] == np.dtype('float64'):
                self.SQLColTypes[col] = 'double'
            #for varchar, get a reasonable length estimate, and leave some room. These are pretty suboptimal, but I want to make sure there's plenty of room:
            if self.SQLColTypes[col] == 'varchar':
                #if it's over 2000, raise an error because it doesn't belong in SQL.
                mask = self.dat[col].str.len() >= 2000
                if not mask.any():
                    self.SQLColTypes[col] = 'varchar(2000)'
                else:
                    print('value in column \''+col+'\' too large!')
                    raise RunTimeError('column value too large for SQL'+col)
                #if it's under 400:
                mask = self.dat[col].str.len() >= 400
                if not mask.any():
                    self.SQLColTypes[col] = 'varchar(500)'
                #if it's under 100:
                mask = self.dat[col].str.len() >= 100
                if not mask.any():
                    self.SQLColTypes[col] = 'varchar(200)'
                #if it's under 50:
                mask = self.dat[col].str.len() >= 50
                if not mask.any():
                    self.SQLColTypes[col] = 'varchar(100)'
                #if it's under 20:
                mask = self.dat[col].str.len() >= 20
                if not mask.any():
                    self.SQLColTypes[col] = 'varchar(40)'
                #if it's under 10:
                mask = self.dat[col].str.len() >= 10
                if not mask.any():
                    self.SQLColTypes[col] = 'varchar(20)'


    #make table for uploading:
    def createSQLTable(self):
        #check if table already exists:
        self.checkForSQL()
        if self.SQL_exists:
            print('Table exists.')
            raise RuntimeError('MySQL table already exists')
        #get column types:
        self.makeSQLColumnTypes()
        if not self.SQLColTypes:
            print('No data to upload.')
            raise RunTimeError('No data to upload')
        #make cursor:
        cursor=self.cnx.cursor()
        query='create table '+self.db+'.'+self.table+' ('
        for col in self.SQLColTypes:
            #no amount of warnings will make columns compliant, so we'll do some rudimentary last-second cleaning:
            query = query + col.replace(' ','_').replace('-','_').replace('(','_').replace(')','_') + ' ' + self.SQLColTypes[col] + ','
        query = query[0:len(query)-1] + (');') #trim unneeded last comma
        cursor.execute(query)
        self.cnx.commit()
        cursor.close()

##################################################################################
##################################################################################
##################################################################################

    #close connection
    def closeConnection(self):
        #WARNING: this will make the object unusable until 'makeConnection()' is run again!
        self.cnx.close()

##################################################################################
##################################################################################
##################################################################################

#uploadFile():

    #removes potential escape characters from strings before uploading
    def SQLizeData(self,dropnulls=False):
        import pandas as pd
        def fixCell(strVal):
            strVal = strVal.replace("'", "''")
            #it'll kill the line breaks... sorry
            strVal = strVal.replace("\n", " ")
            return(strVal)
        #clean out potential escape characters, others that could cause SQL issues
        #make it all strings to make uploading easier
        if dropnulls:
            datUpload = self.dat.where((pd.notnull(self.dat)), None).astype('str') #kills nulls?
        else:
            datUpload = self.dat.astype('str')
        datUpload.apply(fixCell)
        datUpload = datUpload.values.tolist()
        return(datUpload)

    #uploads the file to specified db and table, mildly cleaning to avoid accidental SQL injection. If 'truncate=True', it will truncate target table first.
    def uploadFile(self,truncate=False,dropnulls=False):
        #make cursor:
        cursor=self.cnx.cursor()
        #connect to database:
        try:
            cursor.execute("USE {};".format(self.db))
        except:
            print("Database not found")
            raise
        if dropnulls:
            datUpload=self.SQLizeData(dropnulls=True)
        else:
            datUpload=self.SQLizeData()
        #truncate before loading (if specified):
        if truncate == True:
            cursor.execute('truncate table ' + self.db + '.' + self.table + ';')
        #get columns from target table:
        cursor.execute('show columns from ' + self.db + '.' + self.table + ';')
        cols = cursor.fetchall()
        #draft template insert statement:
        query = ''
        query2 = ''
        count = 0
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
        #upload in one shot with executemany()
        cursor.executemany(query, datUpload)
        self.cnx.commit()
        cursor.close()

##################################################################################
##################################################################################
##################################################################################

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
        #default to False until we see target table
        self.SQL_exists = False
        #see all tables in db, see if target appears
        cursor.execute('show tables;')
        cols=cursor.fetchall()
        for col in cols:
            if col[0]==self.table:
                self.SQL_exists = True
        cursor.close()

##################################################################################
##################################################################################
##################################################################################

    #import credentials for mysql-connector object, validates, connects, returns mysql-connector connection object
    #these should be a .yaml with the following:
    #
    #hostname='foo.bar'
    #username='foo'
    #password='bar'
    #
    #Make sure the config is properly secured on your machine.
    def makeConnection(self):
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

##################################################################################
##################################################################################
##################################################################################

    #Will import all columns.
    def downloadFile(self):
        import pandas as pd
        #first, raise an error if target table doesn't exit:
        #refresh SQL table status first
        self.checkForSQL()
        if not self.SQL_exists:
            print('target table doesnt exist. Nothing to import.')
            raise RuntimeError('target table doesnt exist')
        #make sure self.dat isn't occupied
        if not self.dat.empty:
            print('Data already in memory. Either run \'purgeData()\' or reinstantiate the object.')
            raise RuntimeError('self.dat occupied')
        #make cursor:
        cursor=self.cnx.cursor()
        #fetch data
        cursor.execute('select * from '+self.db+'.'+self.table+';')
        self.dat=pd.DataFrame(cursor.fetchall())
        #make headers meaningful:
        #get column names from SQL
        cursor.execute('show columns from '+self.db+'.'+self.table+';')
        cols=cursor.fetchall()
        #reformat them in a sane way (cursor.fetchall() makes a tuple of tuples):
        colNames=[]
        for col in cols:
            colNames.append(col[0])
        self.dat.columns=colNames

##################################################################################
##################################################################################
##################################################################################

    #purgeData(): empties self.dat, leaving other variables alone
    def purgeData(self):
        import pandas as pd
        self.dat = pd.DataFrame()

##################################################################################
##################################################################################
##################################################################################
