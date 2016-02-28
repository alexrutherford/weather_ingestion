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
import logging
from pymongo import MongoClient

client=MongoClient('localhost')
db=client.open_weather
cleanCollection=db.clean

logging.basicConfig(level=logging.INFO,
                    filename='log.log', # log to this file
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s') 
                    # include timestamp ad function name

files=glob.glob('../data/weather_data/2016_*json')
#files=glob.glob('../data/weather_data/2016_2_19_19*json')
files=[f for f in files if not re.search(r'_processed.json',f)]
logging.info('Found %d files to process' % len(files))

files=[f for f in files if not re.search(r'_processed.json',f)]
logging.info('Found %d new files to process' % len(files))

brazilIndices=np.loadtxt('../data/weather_data/brazil_indices.txt')

dataGen=utils.jsonGenerator(files,verbose=True)

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
