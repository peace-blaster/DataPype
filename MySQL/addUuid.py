#add an uuid
def addUuid(dat):
    import uuid
    #add column
    dat.loc[:,"UUID"]=1
    #add UUID efficiently with a transform:
    dat.loc[0:, "UUID"]=dat.UUID.transform(lambda a: str(uuid.uuid4()))
    #address bug where it UUIDs the cloumn headers
    #dat.iloc[0,len(dat.columns)-1]='UUID'
    return(dat)
