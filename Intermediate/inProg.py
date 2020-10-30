#an intermediate object for holding data between sources and sinks. This is where transformations will take place, as well as union, intersect, etc.

class inProg:
    def __init__(self, importDat):
        #local variables:
        self.dat = importDat
        self.timeColsToFix = []
        #initial functions:
        self.autoFixColNames()

##################################################################################
##################################################################################
##################################################################################

#automatically fix column names to prevent later issues
    def autoFixColNames(self):
        self.dat.columns = self.dat.columns.str.replace(' ','_')
        self.dat.columns = self.dat.columns.str.replace(' ','-')
        self.dat.columns = self.dat.columns.str.replace(' ','(')
        self.dat.columns = self.dat.columns.str.replace(' ',')')

##################################################################################
##################################################################################
##################################################################################

#timestamp fix

    #let user define columns to be fixed:
    def defineTimeColsToFix(self):
        print('columns are:\n')
        for col in self.dat.columns:
            print(col)
        print('\n')
        rawList = input('input comma separated list of cols to correct timestamps on:')
        rawList = rawList.replace(' ','').split(',')
        for col in rawList:
            if col in self.dat.columns:
                self.timeColsToFix.append(col)
            else:
                print('column '+col+' doesnt exist')
                raise ValueError('specified column doesnt exist')

    #fix timestamps as needed
    def dateTransform(self,val):
        from dateutil.parser import parse
        if val != val: #check for NaN again
            return '0000-00-00'
        tst = str(val)
        if tst == 'nan': #not sure how this is possible, but it is
            return '0000-00-00'
        out = parse(val)
        out = out.strftime('%Y-%m-%d %H:%M:%S')
        return out

    #can be called to fix timestamps pandas screwed up
    def applyFixDate(self):
        import numpy as np
        self.defineTimeColsToFix()
        for col in self.timeColsToFix:
            #make string so dateutil can parse:
            self.dat[col] = self.dat[col].astype('str')
            #format correctly
            self.dat[col] = self.dat[col].apply(self.dateTransform)
            #ensure pandas sees it as a timestamp
            self.dat[col] = self.dat[col].astype('datetime64[ns]')

##################################################################################
##################################################################################
##################################################################################

    #intersection with another dataframe:
    def intersection(self,otherDf):
        import pandas as pd
        self.dat = pd.concat(self.dat, otherDf, join='inner')

##################################################################################
##################################################################################
##################################################################################

    #union with another dataframe:
    def union(self,otherDf):
        import pandas as pd
        self.dat = pd.concat(self.dat, otherDf, join='outer')

##################################################################################
##################################################################################
##################################################################################

    #SQL-style join with another dataFrame
    #defaults to "inner" so people don't make devops angry
    #also, only accomodates "left" because I've never seen a right join in my life
    #in SQL terms, this object would be in the "from" clause.
    #To merge the other way, call this from the other object.
    #Also note that 'key' must line up between the objects-
    def join(self,otherDf,key,outer=False):
        import pandas as pd
        if outer:
            self.dat = pd.merge(self.dat, otherDf, on=key, how='left')
        else:
            self.dat = pd.merge(self.dat, otherDf, on=key)

##################################################################################
##################################################################################
##################################################################################

    #add an uuid
    def addUuid(self):
        import uuid
        #add column
        self.dat.loc[:, "UUID"] = 1
        #add UUID efficiently with a transform:
        self.dat.loc[0:, "UUID"] = self.dat.UUID.transform(lambda a: str(uuid.uuid4()))


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
