#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import configparser
from pymongo import MongoClient, errors
import pandas as pd
from datetime import date, datetime
import os
import time

def getAvgMinMaxPersisTime(collectionName, collection):
    pipeline = [
        {"$project": {
            "epoch":       {"$toLong": "${DBInsertDt}".format(DBInsertDt=
                                                             config.get('CHECK_FIELD', 'DBInsertDt'))},
            "createepoch": {"$toLong": "${collection}{ObjectCreationDt}".format(collection=collectionName +".",
                                                                                ObjectCreationDt=
                                                              config.get('CHECK_FIELD', 'ObjectCreationDt'))}
        }},
        {"$project": {
            "difference": {"$subtract": [
                "$epoch",
                "$createepoch"
            ]}
        }},
        {"$group": {
            "_id": None,
            "min": {"$min": "$difference"},
            "max": {"$max": "$difference"},
            "avg": {"$avg": "$difference"}
        }}
    ]

    cursor = collection.aggregate(pipeline)
    result = list(cursor)[0]

    return (result['avg'],
            result['min'],
            result['max']
            )


def getNoOfRecordsUpdated(collection):
    #since records are inserted as UTC(ISO) time in mongoDB, hence calculating current UTC time
    nowTime = time.gmtime()
    reportDt = datetime(nowTime.tm_year, nowTime.tm_mon, nowTime.tm_mday)

    return (reportDt.strftime('%Y%m%d'),
             collection.find({"{DBInsertDt}".format(DBInsertDt=config.get('CHECK_FIELD', 'DBInsertDt')):
                                 {"$gte": reportDt}}).count())


def writeRpt(collectionDetails):
    print('collectionDetails is: ', collectionDetails)
    df = pd.DataFrame(collectionDetails, columns=['CollectionName', 'Date', 'TotalRecords',
                                                  'NoOfRecordsUpdated', 'MinPersistTime(in ms)',
                                                  'MaxPersistTime(in ms) ', 'AvgPersistTime(in ms)'])

    runDate = df.loc[0, 'Date']
    csvFile = os.path.join( config.get('PATH', 'outdir'),
                            'EODReport_for_{db}_{date}.csv'.format(db=config.get('DB', 'database'),
                                                                   date=runDate))

    df.to_csv(csvFile, index=False, encoding='utf-8')

    print('Report path : %s'%csvFile)


def connectDB():
    print('connecting to database...')

    try:
        dbConnectionStr = config.get('DB', 'connectionURI')

        print(dbConnectionStr)

        client = MongoClient(dbConnectionStr)

        print('connected to database.')
        return client
    except errors.ServerSelectionTimeoutError as e:
        print('Error: while connecting to database : ', e.__str__())
        exit(1)


def queryDB(dbClient):
    with dbClient:
        db = dbClient[config.get('DB', 'database')]
        collectionRptDetails = []

        collectionArgList = [collection
                             for collection in config.get('DB', 'collection').split(',')
                             if collection != 'all']

        print('collectionArgList', collectionArgList, bool(collectionArgList))

        for collectionName in db.list_collection_names():
            #if list of collections are passsed from command line, then only those will be considered for reporting
            if collectionArgList and collectionName not in collectionArgList:
                continue
            totalRecords = db[collectionName].count()

            runDate, NoOfRecordsUpdated = getNoOfRecordsUpdated(db[collectionName])

            print('collectionName', collectionName, 'totalRecords', totalRecords,
                  'runDate', runDate, 'NoOfRecordsUpdated', NoOfRecordsUpdated )

            AvgPersistTime, MinPersistTime, MaxPersistTime = getAvgMinMaxPersisTime(collectionName,
                                                                                    db[collectionName])

            collectionRptDetails.append([collectionName, runDate, totalRecords, NoOfRecordsUpdated,
                                 MinPersistTime, MaxPersistTime, AvgPersistTime])

    return collectionRptDetails


if __name__ == '__main__' :
    parser = argparse.ArgumentParser(description='Query DB & prepare report')
    parser.add_argument('-c', '--config', required=True, help='path of config file')

    args = parser.parse_args()

    config = configparser.ConfigParser()

    #reading config file
    config.read(args.config)

    #connecting to DB by getting query String from config file
    dbClient = connectDB()

    #queryinh into DB
    collectionDetails = queryDB(dbClient)

    #writing the report
    if collectionDetails:
        writeRpt(collectionDetails)



