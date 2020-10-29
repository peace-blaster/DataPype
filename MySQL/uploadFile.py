def uploadFile(dat, hostname, username, pword, dbName, dbTable):
    import pandas as pd
    import mysql.connector
    #Fix strings so they don't mess up SQL commands
    def fixCell(strVal):
        strVal=strVal.replace("'", "''")
        strVal=strVal.replace("\n", " ")
        return(strVal)
    def toStr(val):
        val=str(val)
        return(val)
    #connect to mysql:
    try:
        cnx=mysql.connector.connect(user=username
                                , password=pword
                                , host=hostname)
    except mysql.connector.Error as err:
        print("Unable to connect")
        quit()
    #get into the proper db:
    cursor=cnx.cursor()
    try:
        cursor.execute("USE {};".format(dbName))
    except:
        print("Database not found")
        quit()
    #clean out potential escape characters, others that could cause SQL issues
    dat=dat.where((pd.notnull(dat)), None)
    dat.apply(fixCell)
    dat.apply(toStr)
    dat=dat.values.tolist()
    #truncate before loading:
    cursor.execute('truncate table '+dbName+'.'+dbTable+';')
    #get columns from target table:
    cursor.execute('show columns from '+dbName+'.'+dbTable+';')
    cols=cursor.fetchall()
    #draft template insert statement:
    query=''
    query2=''
    count=0
    for i in cols:
        count=count+1
        col='`'+i[0]+'`'
        if count==len(cols): #to avoid comma issues
            query=query+col
            query2=query2+'%s'
        else:
            query=query+col+', '
            query2=query2+'%s, '
    query='INSERT INTO '+dbName+'.'+dbTable+'('+query+')'+' VALUES ('+query2+')'
    print(query)
    print(dat[1])
    cursor.executemany(query, dat)
    cnx.commit()
    cursor.close()
    cnx.close()
