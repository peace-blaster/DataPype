#sample pipeline in straight Python
#######################################################################
#import module components
from Connection import MySQL as ms
from Connection import CSV as ff
from Intermediate import DataTransform as dt
#######################################################################
#episodes- download from MySQL
episodes = ms.MySQL('/home/peaceblaster/Pyformatica/mariadbLogin.yaml', 'IMDB_EXPLORE','EPISODE')
episodes.downloadFile()
episodes.closeConnection()
#ratings- download from MySQL
ratings = ms.MySQL('/home/peaceblaster/Pyformatica/mariadbLogin.yaml', 'IMDB_EXPLORE','RATINGS')
ratings.downloadFile()
ratings.closeConnection()
#######################################################################
#agg1- join 'episodes' and 'ratings'
episode = dt.DataTransform(episodes.dat) #more efficient to overwrite object in the process so we don't hold 'dat' in memory twice
episode.join(ratings.dat, 'episodeId')
ratings.purgeData() #to alleviate load on system
#######################################################################
#outfile- output to filesystem
outfile = ff.CSV('/home/peaceblaster/Pyformatica/tst.csv')
outfile.dat = episode.dat
episode.purgeData() #to alleviate load on system
outfile.outputCSV()
#######################################################################
