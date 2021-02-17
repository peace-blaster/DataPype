#sample pipeline in straight Python
#######################################################################
#import module components
from Connection import MySQL as ms
from Connection import CSV as ff
from Intermediate import DataTransform as dt
#######################################################################
#episodes- download from MySQL
episodes = ms.MySQL('/home/peaceblaster/mariadbLogin.yaml', 'IMDB_EXPLORE','EPISODE')
print('Starting download of episodes from MySQL...')
episodes.downloadFile()
print('Download complete.')
episodes.closeConnection()
#ratings- download from MySQL
ratings = ms.MySQL('/home/peaceblaster/mariadbLogin.yaml', 'IMDB_EXPLORE','RATINGS')
print('Starting download of ratings from MySQL...')
ratings.downloadFile()
print('Download complete.')
ratings.closeConnection()
#######################################################################
#join 'episodes' and 'ratings'
episode = dt.DataTransform(episodes.dat) #more efficient to overwrite object in the process so we don't hold 'dat' in memory twice
print('Merging datasets...')
episode.join(ratings.dat, 'episodeId')
print('Complete.')
ratings.purgeData() #to alleviate load on system
#######################################################################
#output to filesystem
episode = ff.CSV('~/tst.csv', importData=episode.dat) #import efficiently using 'importData' parameter
print("Outputting as CSV to '~/tst.csv'")
episode.outputCSV()
print('Complete.')
#######################################################################
