from pymongo import MongoClient, errors


def connectDB(dbConnectionStr):
    print('connecting to database...')
    client = None

    try:
        print(dbConnectionStr)

        client = MongoClient(dbConnectionStr)

        print('connected to database.')
    except errors.ServerSelectionTimeoutError as e:
        print('Error: while connecting to database : ', e.__str__())

    return client
