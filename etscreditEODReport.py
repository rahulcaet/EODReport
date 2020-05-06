#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import configparser
from pymongo import MongoClient, errors
import pandas as pd
from datetime import date, datetime
import os
import time

def getAvgMinMaxPersisTime(dbClient, collection):
    pipeline = [
        {"$group": {
            "_id": None,
            "avg_time": {
                "$avg": {
                    "$subtract": [

                                    {"$ifNull": ["${DBInsertDt}".format(DBInsertDt=
                                        config.get('CHECK_FIELD', 'DBInsertDt')), 0]},

                                    {"$ifNull": ["${ObjectCreationDt}".format(ObjectCreationDt=
                                        config.get('CHECK_FIELD', 'ObjectCreationDt')), 0]}

                                 ]
                        }
            },
            "max_time": {
                "$max": {
                    "$subtract": [
                                     {"$ifNull": ["${DBInsertDt}".format(DBInsertDt=
                                        config.get('CHECK_FIELD', 'DBInsertDt')), 0]},

                                     {"$ifNull": ["${ObjectCreationDt}".format(ObjectCreationDt=
                                        config.get('CHECK_FIELD', 'ObjectCreationDt')), 0]}

                                 ]
                        }
            },
            "min_time": {
                "$min": {
                    "$subtract": [
                                      {"$ifNull": ["${DBInsertDt}".format(DBInsertDt=
                                        config.get('CHECK_FIELD', 'DBInsertDt')), 0]},

                                      {"$ifNull": ["${ObjectCreationDt}".format(ObjectCreationDt=
                                         config.get('CHECK_FIELD', 'ObjectCreationDt')), 0]}

                                 ]
                        }
            }

            }
        }
    ]

    cursor = collection.aggregate(pipeline)
    result = list(cursor)[0]

    return (result['avg_time'],
            result['min_time'],
            result['max_time']
            )


def getNoOfRecordsUpdated(dbClient, collection):
    #since records are inserted as UTC(ISO) time in mongoDB, hence calculating current UTC time
    nowTime = time.gmtime()
    reportDt = datetime(nowTime.tm_year, nowTime.tm_mon, nowTime.tm_mday)

    return (reportDt.strftime('%Y%m%d'),
             collection.find({"{DBInsertDt}".format(DBInsertDt=config.get('CHECK_FIELD', 'DBInsertDt')):
                                 {"$gte": reportDt}}).count())


def writeRpt(collectionDetails):
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

        dbConnectionStr = '{prefix}://{username}:{password}@{host}:{port}'.format(prefix=config.get('DB', 'prefix'),
                                                                              username=config.get('DB', 'username'),
                                                                              password=config.get('DB', 'password'),
                                                                              host=config.get('DB', 'host'),
                                                                              port=config.get('DB', 'port'))

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
        collectionDetails = []

        for collectionName in db.list_collection_names():
            totalRecords = db[collectionName].count()

            runDate, NoOfRecordsUpdated = getNoOfRecordsUpdated(dbClient,
                                                                db[collectionName])

            print('collectionName', collectionName, 'totalRecords', totalRecords,
                  'runDate', runDate, 'NoOfRecordsUpdated', NoOfRecordsUpdated )

            AvgPersistTime, MinPersistTime, MaxPersistTime = getAvgMinMaxPersisTime(dbClient,
                                                                                    db[collectionName])

            collectionDetails.append([collectionName, runDate, totalRecords, NoOfRecordsUpdated,
                                 MinPersistTime, MaxPersistTime, AvgPersistTime])

    return collectionDetails


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
    writeRpt(collectionDetails)



