#this was stripped out of something much bigger, so it looks like a grandios for its current purpose
#this goes through all columns containing "time", and appropriately formats them for the 'DATETIME' type in MariaDB/MySQL

#find columns that look like timestamps:
def findTimeCols(dat):
    outCols=list()
    for col in dat.columns:
        if 'time' in col:
            outCols.append(col)
    return outCols
def dateTransform(val):
    from dateutil.parser import parse
    if val!=val: #check for NaN again
        return '0000-00-00'
    tst=str(val)
    if tst=='nan': #not sure how this is possible, but it is
        return '0000-00-00'
    out=parse(val)
    out=out.strftime('%Y-%m-%d %H:%M:%S')
    return out
def dateFix(dat):
    cols=findTimeCols(dat)
    for col in cols:
        dat[col]=dat[col].apply(dateTransform)
    return dat
#apply all of the above to the data:
def cleanData(dat):
    dat=dateFix(dat)
    return dat
