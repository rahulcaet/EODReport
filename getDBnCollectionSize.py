def getCollectionSize(dbClient, database, collectionName):
    db = dbClient[database]

    collectionStat = db.command("collstats", collectionName)
    if collectionStat['storageSize'] > (1024*1024):
        unit = 'GB'
        divisor = 1024*1024
    else:
        unit = 'MB'
        divisor = 1024

    collectionSize = str((collectionStat['storageSize']) / divisor) + unit;

    #print('Size of collection is: {}'.format(collectionSize))

    return collectionSize


def getDBSize(dbClient, database):
    db = dbClient[database]

    dbStat = db.command("dbstats")
    if dbStat['storageSize'] > (1024*1024):
        unit = 'GB'
        divisor = 1024*1024
    else:
        unit = 'MB'
        divisor = 1024

    dbSize = str((dbStat['storageSize']) / divisor) + unit

    #print('Size of db is: {}'.format(dbSize))

    return dbSize