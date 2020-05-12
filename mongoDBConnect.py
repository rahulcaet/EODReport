from pymongo import MongoClient, errors
import base64
import logging
import binascii

def connectDB(dbConnectionStr, user, password):
    client = None

    try:
        password = base64.b64decode(password).decode("utf-8")
        dbConnectionStr = dbConnectionStr.replace('user', user).replace('password', password)

    except binascii.Error as e:
        logging.error('Error while decoding password : ' + e.__str__())
        return
    except Exception as e:
        logging.error('Error while decoding password : ' + e.__str__())
        return

    #mongoDB connects asyncronously
    client = MongoClient(dbConnectionStr, connect=False)

    try:
        client.admin.command("ismaster")
    except errors.ConnectionFailure as e:
        logging.error('Error: while connecting to database : %s', e.__str__())
    except errors.ServerSelectionTimeoutError as e:
        logging.error('Error: while connecting to database : %s', e.__str__())
    except Exception as e:
        logging.error('Error: while connecting to database : %s', e.__str__())

    return client
