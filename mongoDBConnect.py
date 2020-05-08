from pymongo import MongoClient, errors
import base64
import logging



def connectDB(dbConnectionStr, user, password):
    client = None

    try:
        password = base64.b64decode(password).decode("utf-8")
        dbConnectionStr = dbConnectionStr.replace('user', user).replace('password', password)
        client = MongoClient(dbConnectionStr)

    except errors.ServerSelectionTimeoutError as e:
        logging.error('Error: while connecting to database : ', e.__str__())
    except Exception as e:
        logging.error('Error: while connecting to database : ', e.__str__())

    return client
