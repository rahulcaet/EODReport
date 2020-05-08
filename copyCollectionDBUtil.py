#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import configparser
from pymongo import MongoClient
import pandas as pd
from datetime import date, datetime
import os

from mongoDBConnect import connectDB
from bson.json_util import dumps, loads
from bson import decode_all


def getSrcCollectionDumpFile(srcdb, collectionName):
    collectionObj = srcdb[collectionName]
    BsonFileName = os.path.join(config.get('PATH', 'workdir'), '%s.json'%collectionName)

    if os.path.exists(BsonFileName):
        try:
            os.unlink(BsonFileName)
        except Exception as e:
            print('Error: removing file ' + BsonFileName + " :" + e.__str__() )
            return 0

    cursor = collectionObj.find({})
    with open(BsonFileName, 'w') as fp:
        fp.write('[')
        qntCursor = 0
        totalDocument = cursor.count()

        for document in cursor:
            qntCursor += 1
            if totalDocument == 1:
                fp.write(dumps(document))
            elif totalDocument > 1 and qntCursor <= (totalDocument-1):
                fp.write(dumps(document))
                fp.write(',')
            elif qntCursor == totalDocument:
                fp.write(dumps(document))

        fp.write(']')

    return BsonFileName

def importBsonTrgDB(trgdb, collectionName, BsonFileName):
    collectionObj = trgdb[collectionName]

    with open(BsonFileName, 'rb') as fp:
        file_data = fp.read()
        file_data = loads(file_data)

    try:
        for document in file_data:
            collectionObj.insert_one(document)
        print('Successfully Imported collection %s'%collectionName)
        return 1
    except Exception as e:
        print('Error: while loading data in target DB for collection ' + collectionName + " :" + e.__str__())
        return 0

def copyCollectionSrcToTrgDB(srcDBClient, trgDBClient):
    print('Inside copyCollectionSrcToTrgDB')

    with srcDBClient:
        srcdb = srcDBClient[config.get('DB', 'database')]
        trgdb = trgDBClient[config.get('DB', 'database')]

        listCollectionNamesTrgDB = trgdb.list_collection_names()

        # check if specific collections passed through cofig to copy
        collectionArgList = [collection
                             for collection in config.get('DB', 'collection').split(',')
                             if collection != 'all']

        for collectionName in srcdb.list_collection_names():
            # only process collections passed from config
            if collectionArgList and collectionName not in collectionArgList:
                continue

            #check if collection exist or empty in target db
            if ((collectionName not in listCollectionNamesTrgDB) or
                (trgdb[collectionName].count() == 0)):
                print('Dumping collection %s to file'%collectionName)
                BsonFileName = getSrcCollectionDumpFile(srcdb, collectionName)

                if BsonFileName:
                    print('Dumping complete for collection %s to file: %s' % (collectionName, BsonFileName))
                    print('Importing in target DB')
                    ret = importBsonTrgDB(trgdb, collectionName, BsonFileName)
                    print('return value: ', ret)
                    trgDBClient.close()
            else:
                print('collection: ' + collectionName + ' already exists in target DB')





if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Query DB & prepare report')
    parser.add_argument('-c', '--config', required=True, help='path of config file')

    args = parser.parse_args()

    config = configparser.ConfigParser()

    #reading config file
    config.read(args.config)

    #connecting to source DB by getting query String from config file
    print('source database connecting...')
    srcDBClient = connectDB(config.get('DB', 'srcConnectionURI'))

    if not srcDBClient:
        print("Error: Connecting to source DB with connection string: %s"%(config.get('DB', 'srcConnectionURI')))
        exit(1)

    print('source database connected.')

    print('target database connecting...')
    trgDBClient = connectDB(config.get('DB', 'trgConnectionURI'))

    if not trgDBClient:
        print("Error: Connecting to target DB with connection string: %s" % (config.get('DB', 'trgConnectionURI')))
        exit(1)

    print('target database  connected.')
    copyCollectionSrcToTrgDB(srcDBClient, trgDBClient)
