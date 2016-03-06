'''
Alex Rutherford
arutherford@unicef.org
Ingestion pipeline for Open Weather map data
http://openweathermap.org/api
'''

import glob
import utils
import re,sys,csv,os
import numpy as np
from dateutil import parser
import pandas as pd
import logging,collections
from pymongo import MongoClient
from params import *

client=MongoClient('localhost')
db=client.open_weather
cleanCollection=db.clean
cleanDailyCollection=db.cleanDaily
stationCollection=db.stations

logging.basicConfig(level=logging.INFO,
                    filename='log.log', # log to this file
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s') 
                    # include timestamp ad function name

files=glob.glob('../data/weather_data/2016_*json')
files=glob.glob('../data/weather_data/2016_2_19_8_*json')
files=[f for f in files if not re.search(r'_processed.json',f)]
logging.info('Found %d files to process' % len(files))

files=[f for f in files if not re.search(r'_processed.json',f)]
logging.info('Found %d new files to process' % len(files))

brazilIndices=np.loadtxt('../data/weather_data/brazil_indices.txt')

stationOffsets=utils.getStationOffsets(stationCollection)
dataGen=utils.jsonGenerator(files,verbose=True,offsets=stationOffsets)

dfAll=pd.concat([d for d in dataGen])
dfUnique=dfAll.drop_duplicates()
res=utils.putDfInMongo(dfUnique,db.clean,bulk=False)
print 'Added %d measurements' % res

#############################
## Rename processed files
map(utils.renameFile,files)
logging.info('Renamed %d files after processing' % len(files))

#############################
## Get date range of new ingested files in db.clean
## Ask if any downsampled measurements indb.cleanDaily overlap with these
## Update those measurements

groups=dfUnique.groupby('id')

earliestInNewData=collections.Counter()

for group in groups:
    resampledGroup=group[1].resample(freq,how='mean',label='left')
    resampledGroup=resampledGroup.dropna(how='any')

    resampledGroup['sensorTime']=resampledGroup.index

    resampledGroup['count']=group[1].resample(freq,how='count',label='left')['id']

    cleanIndex=map(lambda x:pd.datetime(x.year,x.month,x.day,x.hour,x.minute,x.second),group[1].index)

    firstMeasurement=group[1].index.values[0]


    print 'Station %d, first measurement at %s' % (group[0],firstMeasurement)
    lastDBMeasurement=utils.getLastDailyMeasurement(cleanDailyCollection,group[0])
    last=lastDBMeasurement['sensorTime']
    firstMeasurement=pd.datetime.fromtimestamp(int(firstMeasurement)/1e9)

    
    if lastDBMeasurement:
        print '\tLast Measurement in DB %s' % lastDBMeasurement['sensorTime']
        print type(lastDBMeasurement['sensorTime']),type(firstMeasurement)
        
        overlap=group[1][group[1].index<(last+pd.Timedelta(12,'h'))]
        # These are all the raw measurements that fall within the last 
        # aggregated tie bucket in DB
        
        if overlap.shape[0]>0:
            print 'New measurements overlap with last time period',overlap.shape

            utils.updateDailyMeasurement(cleanDailyCollection,overlap,lastDBMeasurement)
        else:
            print 'No overlap'

    print ''


#cur=utils.getMeasurementsInRange(db.clean,pd.datetime(2016,1,1),pd.datetime(2016,3,31))




