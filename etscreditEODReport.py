#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import configparser
from pymongo import MongoClient, errors
import pandas as pd
from datetime import date, datetime
import os
import time
import logging

from mongoDBConnect import connectDB

def getAvgMinMaxPersisTime(collectionName, collection):
    logging.info('Inside getAvgMinMaxPersisTime')
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
    logging.info('Inside writeRpt')
    df = pd.DataFrame(collectionDetails, columns=['DatabaseName', 'CollectionName', 'Date',
                                                  'TotalRecords', 'NoOfRecordsUpdated',
                                                  'MinPersistTime(in ms)', 'MaxPersistTime(in ms)',
                                                  'AvgPersistTime(in ms)', 'CollectionSize(in MB)'])

    columns = eval(config.get('RPT_COLUMNS', 'columns'))

    if ('all' not in columns):
        #subset of columns
        df = df[columns]

    runDate = df.loc[0, 'Date']
    csvFile = os.path.join(config.get('PATH', 'outdir'),
                           'EODReport_for_{date}.csv'.format(date=runDate))

    df.to_csv(csvFile, index=False, encoding='utf-8')

    logging.info('Report path : %s'%csvFile)


def getCollectionSize(db, collectionName):
    collectionStat = db.command("collstats", collectionName)
    collectionSize = str((collectionStat['storageSize']) / (1024*1024)) + 'MB'

    #print('Size of collection is: {}'.format(collectionSize))
    return collectionSize


def getDBSize(db):
    dbStat = db.command("dbstats")
    dbSize = str((dbStat['storageSize']) / (1024*1024)) + 'MB'

    #print('Size of db is: {}'.format(dbSize))

    return dbSize

def queryDB(dbClient):
    logging.info('Inside queryDB')

    collectionRptDetails = []

    #get all DB sections names
    dbArgDict = {config.get(section, 'database'): section
                 for section in config.sections() if section.startswith('DB')}

    print('dbArgDict:', dbArgDict)
    #for dbSection in dbSections:
    for entry in dbClient.list_databases():
        databaseName = entry.get('name')
        #print('databaseName: ', databaseName)
        dbSection = dbArgDict.get(databaseName) if databaseName in dbArgDict else dbArgDict.get('all')

        #print('databaseName= ', databaseName, 'dbSection= ',  dbSection)
        if not dbSection:
            continue

        db = dbClient[databaseName]

        collectionArgList = [collection
                             for collection in config.get(dbSection, 'collection').split(',')
                             if collection != 'all']

        logging.info('in Database %s collectionArgList %s' % (databaseName, collectionArgList))

        for collectionName in db.list_collection_names():
            #if list of collections are passsed from command line, then only those will be considered for reporting
            if collectionArgList and collectionName not in collectionArgList:
                continue
            totalRecords = db[collectionName].count()

            runDate, NoOfRecordsUpdated = getNoOfRecordsUpdated(db[collectionName])

            logging.info('collectionName {}, totalRecords {}, runDate {}, NoOfRecordsUpdated{}'.format(collectionName,
                                                                                                totalRecords,
                                                                                                runDate,
                                                                                                NoOfRecordsUpdated ))

            AvgPersistTime, MinPersistTime, MaxPersistTime = getAvgMinMaxPersisTime(collectionName,
                                                                                    db[collectionName])

            logging.info('AvgPersistTime {}, MinPersistTime {}, MaxPersistTime {}'.format(AvgPersistTime,
                                                                                          MinPersistTime,
                                                                                          MaxPersistTime))

            collectionSize = getCollectionSize(db, collectionName)

            logging.info('Database {} Collection {} size : {}'.format(databaseName, collectionName, collectionSize))

            collectionRptDetails.append([databaseName, collectionName, runDate, totalRecords,
                                         NoOfRecordsUpdated, MinPersistTime, MaxPersistTime,
                                         AvgPersistTime, collectionSize])

    return collectionRptDetails


if __name__ == '__main__' :
    parser = argparse.ArgumentParser(description='Query DB & prepare report')
    parser.add_argument('-c', '--config', required=True, help='path of config file')
    parser.add_argument('-d', '--dbconfig', required=True, help='path of DB config file which contains credentials')

    args = parser.parse_args()

    config = configparser.ConfigParser()

    # dbconfig parser instance
    dbConfig = configparser.ConfigParser()

    #reading config file
    config.read(args.config)

    # reading db config file
    dbConfig.read(args.dbconfig)

    # setting the logger
    logFileName = os.path.join(config.get('PATH', 'logdir'), __file__.replace('.py', '.log'))
    logging.basicConfig(filename=logFileName,
                        format='%(asctime)s %(message)s',
                        filemode='w',
                        level=logging.INFO)

    #connecting to DB by getting query String from config file
    logging.info('connecting database...')
    dbClient = connectDB(dbConfig.get('DB', 'srcConnectionURI'),
                         dbConfig.get('DB', 'srcUser'),
                         dbConfig.get('DB', 'srcPassword'))

    if not dbClient:
        logging.error("Error: Connecting to DB with connection string: %s"
                      % (dbConfig.get('DB', 'srcConnectionURI')))
        exit(1)

    logging.info('database connected.')

    #querying into DB
    collectionDetails = queryDB(dbClient)

    #writing the report
    if collectionDetails:
        writeRpt(collectionDetails)




