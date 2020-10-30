# DataPype
Libraries to alleviate typing, debugging, and general headache from Python-driven DI pipelines.
Documention for Python DI project:

#Overview:

This is a set of libraries intended to alleviate a substantial amount of typing, debugging, and general headache from Python scripting DI pipelines. Should a front end or full stack developer assist, it could even become a GUI web tool like Informatica. Once this module is complete, a final product should never exceed 100 lines of code (less if not annotated) except in the most complex of circumstances, and shouldn’t require much intellectual effort or prior knowledge.

From an engineering perspective, this is essentially a bunch of decorator classes masking pandas and mysql-connector. While this may seem unnecessary, the speed, and ease-of-use are worthwhile. The base objects are kept within the introduced classes, and easily accessible and mutable should the simplistic style of this module prove insufficient for a given task. It also makes interactive ETL via the python3 shell practical.

#Object types:

##Connections:
These represent connections to outside Python, such as database systems and flat files. Like all objects in the module, they hold pandas data frames for immediate storage, but unlike Intermediate data objects, they have methods for retrieving or depositing their contents outside the pipeline. They are intended to be the endpoints of the pipeline, and can serve as a source or sink.

###Intermediates:
These are used to store data between Connections in the pipeline, and are outfitted with numerous methods for filtering, cleaning, and merging data.

#Object and Method Specifics:

##MySQL:
a Connection object built around the mysql-connector module. It contains a pandas data frame, and maintains a mysql connection confirmed to work with MySQL and MariaDB. Takes connection information from a .yaml file provided when instantiating the class, and the target database and table double as either the target table for downloading data, or uploading, and if the table is nonexistent, it can be created.

###Creation:

####MySQL(path, database, table):
creates the object itself. It will create and test database connection on creation, as well as determine if target table already exists.
- path: a path to the required .yaml file with the hostname, username, and password to access the RDBMS.
- database: the target database in MySQL. Must exist upon instantiation.
- table: the target table within the target database. Can be created with ‘createSQLTable()’ if needed.

####Methods:

self.createSQLTable(): if target table doesn’t exist, it runs ‘makeSQLColumnTypes()’ to determine suitable MySQL data types, and creates a database with identical column names on the target database system. Note that predetermined data types will over allocate space intentionally in an effort to future proof, and will not tolerate strings greater than 2000 characters, as this seemed impractical. Will raise an exception if target table already exists, or if contained dataframe is empty.

#####self.closeConnection(): closes the MySQl connection (‘cnx’). This is advised if done uploading/downloading data. Can be reconnected with ‘makeConnection()’.

#####self.checkForSQL(): checks whether ‘db’.’table’ exists on the connected host. Updates ‘SQL_exists’ as opposed to returning boolean.

#####self.makeSQLColumnTypes(): analyzes data types in contained dataframe ‘dat’, guesses suitable SQL data types for each column, and stores them as a dict ‘SQLColTypes’. Used in ‘createSQLTable()’.

#####self.SQLizeData(dropNulls=False): creates and returns additional data frame object appropriately sanitized for use in ‘uploadFile()’.
- dropnulls: if true, will drop all rows in dataframe with null values. Defaults to ‘False’.

#####self.uploadFile(truncate=False, dropNulls=False): uploads data from contained dataframe to target table. Runs ‘SQLizeData’ first to sanitize input.
- dropnulls: if true, will drop all rows in data frame with null values. Defaults to ‘False’.
- truncate: if true, will truncate target table prior to upload. Defaults to ‘False’.

#####self.makeConnection(): opens prerequisite .yaml file with connection info, and creates database connection object via mysql-connector. This is done automatically upon instantiation of MySQL object, but may be refreshed manually via this command. Credentials are not kept in memory outside the scope of this function, and possibly the contained connection object.

#####self.downloadFile(): populates contained dataframe with the contents of target table. Will fail if contained dataframe is not empty. Dataframe can be emptied via ‘purgeData()’.

#####self.purgeData(): initializes contained dataframe.

####Attributes:

#####self.dat: the contained pandas dataframe for data storage within the object itself. Can be emptied via ‘purgeData()’, and can be populated by either ‘downloadFile()’, or by directly assigning data with ‘<class instance>.dat=<some pandas dataframe>’.

#####self.filePath: path to the needed .yaml file with MySQL connection info.

#####self.cnx: the mysql-connector object created by ‘makeConnection()’. Will be made automatically when instantiating the class.

#####self.db: name of database on target host system containing target table.

#####self.table: name of target table on host system. Should not include containing database/schema- for example, ‘employees.payroll_tbl’ should have “self.db=‘employees’”, and “self.table=‘payroll_tbl’”.

#####self.SQL_exists: boolean for whether target table exists on connected host. Is assessed on instantiation, and can be reassessed via ‘checkForSQL()’.

#####self.SQLColTypes: dict containing column names from ‘self.dat’ and guesses at appropriate MySQL datatypes made via ‘makeSQLColumnTypes()’.


##CSV: 
A Connection object for importing and exporting to .csv files. Requires a path to target file, regardless of existence (if it doesn’t yet exist, it can be created and written to).

###Creation:

#####CSV(self, filepath, filesep = ‘,’): creates object itself, and checks for file at ‘filepath’ (stored in object as ‘self.fpath’).
- filepath: path to target .csv file- stored as ‘self.fpath’.
- filesep: separator/deliminator for .csv file- stored as ‘self.sep’. Defaults to ‘,’.


###Methods:

#####checkForFile(): checks where target .csv file exists at ‘self.fpath’

#####getFile(noHeader=False): imports data from target .csv file at ‘self.fpath’ using separator ‘self.sep’, and stores in ‘self.dat’. If ‘self.dat’ is nonempty, and error is raised.
- noHeader: if true, first row of target .csv will be interpreted as data, and column names will be populated generically as ‘c1’, ‘c2’, … in ‘self.dat’.

#####purgeData(): initializes ‘self.dat’

#####outputCSV(overwrite=False): exports contents of ‘self.dat’ into target .csv at ’self.path’, deliminated by ‘self.sep’. If file exists there, an error is raised.
- overwrite: if true, method will overwrite file at ‘self.path’ if one is found instead of raising an error.

###Attributes:

#####self.dat: the contained pandas dataframe for data storage within the object itself. Can be emptied via ‘self.purgeData()’, and can be populated by either ‘getFile()’, or by directly assigning data with ‘<class instance>.dat=<some pandas dataframe>’.

#####self.fpath: the path to target .csv file.

#####self.sep: the deliminator used for target .csv file.

#####Self.File_exists: boolean determining whether target .csv at ‘self.fpath’ currently exists. Is set by ‘checkForFile()’.


##DataTransform: 
An Intermediate object used for the majority of data manipulation. Has methods for merging with other dataframes, as well as cleaning and transforming contained data. Must be instantiated with existing data from a Connection object. Also note that like any object in the model, the dataframe is exposed as ‘self.dat’ for any manipulations not yet implemented as methods.

###Creation:

#####DataTransform(importDat): creates the object, imports ‘importDat’ as ‘self.dat’, and runs ‘autoFixColNames()’ to ensure future compatibility with other Connection objects.
- importDat: target pandas dataframe to import

###Methods:

#####autoFixColNames(): replaces ‘-‘,’ ’,’(‘,’)’ in column names with ‘_’ to prevent issues with majority of Connection objects.

#####defineTimeColsToFix(): takes user input to define set of target columns for ‘fixDatetimes()’. ‘fixDatetimes()’ runs this automatically when called. This shouldn’t be called directly.

#####dateTransform(val): used in fixDates. This shouldn’t be called directly.

#####timeTransform(val): used in fixDates. This shouldn’t be called directly.

#####fixDates(notime=False): takes user input using ‘defineTimeColsToFix()’, and attempts to convert wayward values to ‘YYYYMMDD HHMMSS’.
- notime: if true, output will truncate timestamp portion, and provide just ‘YYYYMMDD’. 

#####intersection(otherDf): performs a SQL-like intersection operation of ‘self.dat’, and ‘otherDF’, then stores the result as ‘self.dat’. In particular, this actually performs a pandas ‘pandas.concat()’ with “join=‘inner’”.
- otherDF: target pandas dataframe for merging.

#####union(otherDf): performs a SQL-like union operation of ‘self.dat’, and ‘otherDF’, then stores the result as ‘self.dat’. In particular, this actually performs a pandas ‘pandas.concat()’ with “join=‘outer’”.
- otherDF: target pandas dataframe for merging.

#####join(otherDf): performs a SQL-like join operation of ‘self.dat’, and ‘otherDF’, then stores the result as ‘self.dat’. In particular, this actually performs a pandas ‘pandas.merge()’ on ‘key’, defaulting to ‘inner’ behavior.
- otherDF: target pandas dataframe for merging.
- key: common column on which to join. In SQL terms, this appears as ‘…ON A.key=B.key’
- outer: if true, performs SQL-style left join as opposed an inner. Defaults to ‘False’. 

#####purgeData(): initializes ‘self.dat’

#####addUuid(): adds a column to ‘self.dat’ called ‘UUID’, and populates it with uuid values generated from the uuid module, in particular ‘uuid.uuid4()’.
