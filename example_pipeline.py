#sample pipeline in straight Python
#######################################################################
#import module components
from Sources import MySQL as ms
from Sources import CSV as ff
from Intermediate import inProg as ip
#######################################################################
#episodes- download from MySQL
episodes = ms.MySQL('/home/peaceblaster/Pyformatica/mariadbLogin.yaml', 'IMDB_EXPLORE','EPISODE')
episodes.downloadFile()
#ratings- download from MySQL
ratings = ms.MySQL('/home/peaceblaster/Pyformatica/mariadbLogin.yaml', 'IMDB_EXPLORE','RATINGS')
ratings.downloadFile()
#######################################################################
#agg1- join 'episodes' and 'ratings'
agg1 = ip.inProg(episodes.dat)
episodes.purgeData() #to alleviate load on system
agg1.join(ratings.dat, 'episodeId')
ratings.purgeData() #to alleviate load on system
#######################################################################
#outfile- output to filesystem
outfile = ff.CSV('/home/peaceblaster/Pyformatica/tst.csv')
outfile.dat = agg1.dat
agg1.purgeData() #to alleviate load on system
outfile.outputCSV()
#######################################################################
