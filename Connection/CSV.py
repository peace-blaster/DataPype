#an oject for importing/exporting .csv files.

class CSV:
    #init- no mandatory parameters! where this is used coming or going, it needs to be flexible
    def __init__(self, filepath, filesep = ',', importData = []):
        #where this is intended to be used coming and going, dat will be initialized empty. Populate it via 'objName.dat=<dataframe>', or 'getFile()'
        #it will expect meaningful, and SQL-compliant column names. The automatic cleaning is limited at best, so please clean headers before import.
        import pandas as pd
        self.dat = importData
        #path to file:
        self.fpath = filepath
        #deliminator for .csv file:
        self.sep = filesep
        #needed to see if SQL table exists:
        self.File_exists = False

        #run startup tasks:
        #see if file exists at path:
        self.checkForFile()
        #if no input provided, make it a dataframe
        if self.dat.isempty:
            self.dat = pd.DataFrame()


##################################################################################
##################################################################################
##################################################################################

    #checkForFile(): see if file is there

    def checkForFile(self):
        import pandas as pd
        self.File_exists = False
        tst=pd.DataFrame()
        try:
            tst = pd.read_csv(self.fpath, sep = self.sep)
        except IOError:
            #hopefully bugs don't hide in here
            self.File_exists = False
        except:
            print('something else went wrong')
            raise
        if not tst.empty:
            self.File_exists = True

##################################################################################
##################################################################################
##################################################################################

    #getFile(): import file from fpath
    #occasionally you will need pandas to not read the first line as column names- this often happens with awk output. the noHeader parameter allows you to do this.
    #note however that this will leave null strings for the column names
    def getFile(self,noHeader=False):
        import pandas as pd
        #make file exists
        self.checkForFile()
        #make sure self.dat isn't occupied
        if not self.dat.empty:
            print('Data already in memory. Either run \'purgeData()\' or reinstantiate the object.')
            raise RunTimeError('self.dat occupied')
        #get file
        if self.File_exists:
            if noHeader:
                self.dat = pd.read_csv(self.fpath, sep = self.sep, header = None)
                #ADD SOMETHING TO POPULATE COLUMNS- THIS LEAVES IT LITERALLY EMPTY! NOT EVEN A LIST!
                cols=[]
                for i in range(len(self.dat.iloc[1,:])):
                    cols.append('c'+str(i))
                self.dat.columns=cols
            else:
                self.dat = pd.read_csv(self.fpath, sep = self.sep)
                #clean columns
                self.dat.columns = self.dat.columns.str.replace(' ','_')
                self.dat.columns = self.dat.columns.str.replace(' ','-')
                self.dat.columns = self.dat.columns.str.replace(' ','(')
                self.dat.columns = self.dat.columns.str.replace(' ',')')
        else:
            print('file not found')
            raise FileNotFoundError('csv')

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

    #outputCSV(): output data as .csv with the input deliminator
    def outputCSV(self, overwrite = False):
        import pandas as pd
        #make file exists
        self.checkForFile()
        #make sure self.dat isn't occupied
        if self.dat.empty:
            print('No data to write.')
            raise RuntimeError('self.dat.empty')
        if overwrite or not self.File_exists:
            self.dat.to_csv(self.fpath, sep = self.sep, index = False, header = True)
        else:
            print('File exists. Rerun with \'overwrite=True\' to override.')
