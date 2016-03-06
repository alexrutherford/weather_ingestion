import requests
import pymongo
import sys
import time
from secrets import TIMEZONEKEY

client=pymongo.MongoClient('localhost')

db=client.open_weather

stationCollection=db.stations


base='http://api.timezonedb.com/'

#########################
def getZone(lat,lng):

    url=base+'?lat=%f&lng=%f&key=%s&format=json' % (lat,lng,TIMEZONEKEY)
    res=requests.get(url)
    return res

cur=stationCollection.find({'country':{'$exists':False}})
# Skip stations we did already

lats=[]
longs=[]
ids=[]

#################
for c in cur:
#################
    print c

    try:
    	dummyLat=c['lat']
    	dummyLong=c['long']
    	dummyId=c['id']
        lats.append(dummyLat)
        longs.append(dummyLong)
   	ids.append(dummyId)
    except:
 	print 'Error reading DB'

    res=getZone(dummyLat,dummyLong)

    if res.status_code==200 and res.json()['status']==u'OK':
	res=res.json()
        res=stationCollection.update_one({'id':dummyId},{'$set':{'country':res['countryCode'],'offset':res['gmtOffset']}})
	print dummyId
    else:
        print 'API error %d' % dummyId
    time.sleep(2)
