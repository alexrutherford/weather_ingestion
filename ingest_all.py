'''
Alex Rutherford
arutherford@unicef.org
Ingestion pipeline for Open Weather map data
http://openweathermap.org/api
'''

import glob
import utils
import re,sys,csv,os,timeit
import numpy as np
from dateutil import parser
import pandas as pd
import logging
from pymongo import MongoClient
from params import *

client=MongoClient('localhost')
db=client.open_weather
cleanCollection=db.clean
stationCollection=db.stations

logging.basicConfig(level=logging.INFO,
                    filename='log.log', # log to this file
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s') 
                    # include timestamp ad function name

#######################
## Read in data from files
files=glob.glob('../data/weather_data/2016_*json')
#files=glob.glob('../data/weather_data/2016_2_19_19*json')
logging.info('Found %d files to process' % len(files))

files=[f for f in files if not re.search(r'_processed.json',f)]
logging.info('Found %d new files to process' % len(files))

brazilIndices=np.loadtxt('../data/weather_data/brazil_indices.txt')

dataGen=utils.jsonGenerator(files,verbose=True)

try:
    dfAll=pd.concat([d for d in dataGen])
except:
    print 'Error concatentating dataframes. No files to process? (%d)' % len(files)
    sys.exit(1)

############################
## Take unique tower information and put in collection
dfUnique=dfAll.drop_duplicates()
dfStation=pd.DataFrame(data={'lat':dfUnique['lat'],'long':dfUnique['long'],'id':dfUnique['id']})
dfStation=dfStation.drop_duplicates()
dfStation.set_index(dfStation['id'],inplace=True)
# Mark which towers are in Brazil
dfStation=dfStation.set_value(brazilIndices,'inBrazil',True)

res=utils.putDfInMongo(dfStation,stationCollection)
print 'Imported %d weather stations' % res
print '%f weather stations in Brazil' % sum(dfStation['inBrazil'])

############################
## Import measurements
del dfAll['lat']
del dfAll['long']
# Don't need these to be stored for each measurements

res=utils.putDfInMongo(dfUnique,db.clean)
print 'Added %d measurements' % res
# Inserts in bulk, not individually
# Assumes files are not processed incrementally
# So duplicate rows can be removed before insertion
# If bulk insertion becomes too large, change bulk flag to True
# in putDfInMongo()

###########################
## Retrieve some measurements in a given time range
start=timeit.timeit()
cur=utils.getMeasurementsInRange(db.clean,pd.datetime(2016,1,1),pd.datetime(2016,3,31))
end=timeit.timeit()
print 'Got %d rows in selected time period. Took %d' % (cur.count(),end-start)
###########################
## Put downsampled (12h) unique data into new collection
groups=dfUnique.groupby('id')

nGroups=0

for group in groups:
    resampledGroup=group[1].resample(freq,how='mean',label='left')
    resampledGroup['sensorTime']=resampledGroup.index
    resampledGroup['count']=group[1].resample(freq,how='count',label='left')['id']
    # Store how manyindividual measurements went into downsample
    res=utils.putDfInMongo(resampledGroup,db.cleanDaily)
    assert res==resampledGroup.shape[0], 'Error importing station %d: %d (%d,%d)' % (group[0],len(res.inserted_ids),resampledGroup.shape[0],resampledGroup.shape[1])

    nGroups+=1

print 'Imported data from %d resampled weather stations' % nGroups

#############################
## Rename processed files
map(utils.renameFile,files)
logging.info('Renamed %d files after processing' % len(files))
