#sample pipeline in straight Python
#######################################################################
#import module components
from Connection import MySQL as ms
from Connection import CSV as ff
from Intermediate import DataTransform as dt
from etc import readYamlCred as ryc
#######################################################################
#get db credentials from .yaml:
dbCred=ryc.readYamlCred('mariadbLogin.yaml')
#episodes- download from MySQL
episodes = ms.MySQL(username=dbCred['username'], hostname=dbCred['hostname'], password=dbCred['password'], dbName='IMDB_EXPLORE')
print('Starting download of episodes from MySQL...')
episodes.runQuery('select * from IMDB_EXPLORE.EPISODE limit 200;')
print(episodes.dat.dtypes) #pandas dataframes can still be accessed like this
print('Download complete.')
episodes.closeConnection()
#ratings- download from MySQL
ratings = ms.MySQL(username=dbCred['username'], hostname=dbCred['hostname'], password=dbCred['password'], dbName='IMDB_EXPLORE')
print('Starting download of ratings from MySQL...')
ratings.runQuery('select * from IMDB_EXPLORE.RATINGS;')
print(ratings.dat.dtypes) #pandas dataframes can still be accessed like this
print('Download complete.')
ratings.closeConnection()
#######################################################################
#join 'episodes' and 'ratings'
#this could be done with SQL above, but for demonstration purposes, we're doing it with Python
episodes = dt.DataTransform(episodes.dat) #more efficient to overwrite object in the process so we don't hold 'dat' in memory twice
print('Merging datasets...')
episodes.join(ratings.dat, 'episodeId')
episodes.addUuid()
print('Complete.')
ratings.purgeData() #to alleviate load on system
#######################################################################
#output to filesystem
episodes = ff.CSV('~/tst.csv', importData=episodes.dat)
print("Outputting as CSV to '~/tst.csv'")
episodes.outputCSV()
print('Complete.')
#######################################################################
#upload to the database:
episodes = ms.MySQL(username=dbCred['username'], hostname=dbCred['hostname'], password=dbCred['password'], dbName='TST', importData=episodes.dat)
print('Beginning upload.')
episodes.uploadData('tst',truncate=True)
print('Upload complete.')
