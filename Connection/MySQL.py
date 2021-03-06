#This object exists to represent a MySQL or MariaDB connection (really, anything compatible with mysql-connector).
#It holds a pandas dataframe for temporary data storage, along with methods for uploading and downloading data, as well as creating and preparing tables for such.
import pandas as pd
import numpy as np
import mysql.connector
class MySQL:
    #importData can be used to drastically reduce memory usage by reinstantiating objects over themselves- e.g. exampleObj=MySQL(self,username,password,hostname,dbName,importData=exampleObj.dat)
    #this can be done with any object in this module
    def __init__(self,username='',password='',hostname='',dbName='',importData=pd.DataFrame([])):
        #where this is intended to be used coming and going, dat will be initialized empty. Populate it via 'objName.dat=<dataframe>' (if worried about memory usage, instead pass in data through the 'importdata' parameter on creation).
        #it will expect meaningful, and SQL-compliant column names. The automatic cleaning is limited at best.
        if not type(importData)=='str': #I know this is hacky, but pandas doesn't use bool()
            self.dat = importData
        #if no input provided, make it a dataframe
        else:
            self.dat = pd.DataFrame()

        #the database we will use
        #note you can change this later if you want to upload back to the same host
        self.db = dbName

        #to store SQL column types when going pandas --> SQL. Pandas does a good enough job figuring it out when going SQL --> pandas.
        self.SQLColTypes=dict()

        #login info- working on a better solution for this
        self.username = username
        self.password = password
        self.hostname = hostname

        #connect
        self.cnx = ''
        self.makeConnection()

##################################################################################
##################################################################################
##################################################################################

    #objectively, guessing good column types will be difficult
    #best we can do is be a little generous going off of pandas.dtypes
    #it would be possible to use MySQL's built-in optimizer to alter table after creation for optimal types, but it tends to overuse enum, and is too aggressive to be future-proof.
    def makeSQLColumnTypes(self):
        #initialize to prevent issues:
        self.SQLColTypes=dict()
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
    def createSQLTable(self,table):
        #check if table already exists:
        if self.checkForSQL(table):
            print('Table exists.')
            raise RuntimeError('MySQL table already exists')
        #get column types:
        self.makeSQLColumnTypes()
        if len(self.dat.columns) == 0:
            print('No data to upload.')
            raise RuntimeError('No data to upload')
        #make cursor:
        cursor=self.cnx.cursor()
        query='create table '+self.db+'.'+table+' ('
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

    #uploads the file to specified db and table, mildly cleaning to avoid accidental SQL injection. If 'truncate=True', it will truncate target table first.
    def uploadData(self,table,truncate=False,dropnulls=False):
        #make function for later:
        def fixCell(strVal):
            strVal = strVal.replace("'", "''")
            #it'll kill the line breaks... sorry
            strVal = strVal.replace("\n", " ")
            return(strVal)
        #make cursor:
        cursor=self.cnx.cursor()
        #connect to database:
        try:
            cursor.execute("USE {};".format(self.db))
        except:
            print("Database not found")
            raise
        #make table if it doesn't exist
        if not self.checkForSQL(table):
            self.createSQLTable(table)
        #truncate if prompted
        if truncate == True:
            cursor.execute('truncate table '+self.db+'.'+table+';')
        #get columns from target table:
        cursor.execute('show columns from '+self.db+'.'+table+';')
        cols = cursor.fetchall()
        #draft template insert statement:
        preamble='INSERT INTO '+self.db+'.'+table+'('
        for col in self.dat.columns:
            preamble=preamble+col+','
        #trim the extra comma
        preamble=preamble[0:-1]+') VALUES ('
        count=0
        #load them up, be careful about inserts
        for i, row in self.dat.iterrows():
            count=count+1
            query=preamble
            #determine if quotes are needed based on data type:
            for col in row:
                #if col.dtype in (np.dtype('int64'),np.dtype('int16'),np.dtype('int8'),np.dtype('int_'),np.dtype('float64'),np.dtype('float32'),np.dtype('float16')):
                if not type(col) is str:
                    #catch nans
                    if np.isnan(col):
                        query=query+'NULL'+','
                    else:
                        query=query+str(col)+','
                else:
                    #make sure strings don't have line breaks or escape characters
                    query=query+"'"+str(fixCell(col))+"',"
            query=query[0:-1]+');'
            cursor.execute(query)
            #commit every 10000 rows:
            if count==10000:
                self.cnx.commit()
        self.cnx.commit()
        cursor.close()


##################################################################################
##################################################################################
##################################################################################

    #checkForSQL: tests whether SQL table exists.
    def checkForSQL(self, table):
        #make cursor:
        cursor=self.cnx.cursor()
        #connect to database:
        try:
            cursor.execute("USE {};".format(self.db))
        except:
            print("Database not found")
            raise
        #see all tables in db, see if target appears
        cursor.execute('show tables;')
        cols=cursor.fetchall()
        for col in cols:
            if col[0]==table:
                return True
        return False
        cursor.close()

##################################################################################
##################################################################################
##################################################################################

    def makeConnection(self):
        #try connection:
        try:
            self.cnx=mysql.connector.connect(user=self.username
                                    , password=self.password
                                    , host=self.hostname)
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
    def runQuery(self,query):
        import pandas as pd
        #make sure self.dat isn't occupied
        if not len(self.dat.columns) == 0:
            print('Data already in memory. Either run \'purgeData()\' or reinstantiate the object.')
            raise RuntimeError('self.dat occupied')
        #make cursor:
        cursor=self.cnx.cursor()
        #fetch data
        cursor.execute(query)
        self.dat=pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

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
