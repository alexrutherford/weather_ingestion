# coding: utf-8
import pymongo
from pymongo import MongoClient

client=MongoClient('localhost')
db=client.open_weather
cleanCollection=db.clean
cleanCollection.create_index([('sensorTime',pymongo.ASCENDING),('id',pymongo.ASCENDING)],unique=True)
# Default index '_id' must persist

cleanDailyCollection=db.cleanDaily
cleanDailyCollection.create_index([('sensorTime',pymongo.ASCENDING),('id',pymongo.ASCENDING)],unique=True)

stationCollection=db.stations
stationCollection.create_index([('id',pymongo.ASCENDING)],unique=True)

print 'New DB created'
