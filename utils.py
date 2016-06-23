import math
import numpy as np
import logging,traceback,json
from dateutil import parser
import pandas as pd
import os,re,collections
import pymongo

earthRadius=6371.0
# KMs
logging.basicConfig(level=logging.INFO,
                    filename='log.log', # log to this file
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s') 


############
def updateDailyMeasurement(cleanDailyCollection,overlap,lastDBMeasurement):
############
    '''If new measurements overlap with aggregatedmeasurements in DB
    add overlaps together and weight according to number of measurements
    '''

    print 'Update',lastDBMeasurement

    print 'With',overlap

    print 'Weight %d : %d',lastDBMeasurement['count'],overlap.shape[0]

    oldWeight=lastDBMeasurement['count']
    newWeight=overlap.shape[0]

    newDBMeasurement=lastDBMeasurement

    for k in ['temperature','pressure','humidity']:
        newDBMeasurement[k]=(oldWeight*lastDBMeasurement[k]+overlap[k].sum())/float(oldWeight+newWeight)

    newDBMeasurement['count']+=newWeight

    print 'Merged to',newDBMeasurement

    res=cleanDailyCollection.find_one_and_update({'id':newDBMeasurement['id'],'sensorTime':lastDBMeasurement['sensorTime']}\
            ,{'$set':{'count':newDBMeasurement,'temperature':newDBMeasurement['temperature'],'humidity':newDBMeasurement['humidity'],\
            'pressure':newDBMeasurement['pressure']}},return_document=pymongo.ReturnDocument.AFTER)
    
    print 'Updated to',res

############
def getStationOffsets(collection):
############
# db.stations.find({'offset':{$exists:true}},{'_id':0,'id':1,'offset':1,'country':1})

    cur=collection.find({'offset':{'$exists':True}},{'_id':0,'id':1,'offset':1})
    
    offsetDict=collections.defaultdict(int)
    
    nCount=0

    for l in cur:
	offsetDict[l['id']]=int(l['offset'])
        nCount+=1

    logging.info('Got %d station time zone offsets' % nCount)
    if nCount==0:
        logging.critical('Found zero station offsets. Run timezone script?')

    return offsetDict

############
def unRenameFile(f):
############
    '''
    Convenience function to rename files
    after processing back to original, 
    assumes .json extension
    '''
    newPath=re.sub('_processed.json','.json',f)
    os.rename(f,newPath)

############
def renameFile(f):
############
    '''
    Convenience function to rename files
    after processing, assumes .json extension
    '''
    newPath=re.sub('.json','_processed.json',f)
    os.rename(f,newPath)

############
def getLastDailyMeasurement(cleanDailyCollection,station):
############
#    cur=db.cleanDaily.findOne({},)
   cur=cleanDailyCollection.find({'id':station}).sort('sensorTime',-1).limit(1) 
   if cur.count()==0:
       print 'Station missing'
   else:
       return cur.next()
############
def getMeasurementsInRange(collection,start,end,tower=None):
############
    '''
    Queries collection for measurements in time range
    and optional specific tower
    returns cursor
    '''

    if tower:
        cur=collection.find({"sensorTime":{"$lt":end,"$gte":start},"id":tower})
    else:
        cur=collection.find({"sensorTime":{"$lt":end,"$gte":start}})

    return cur

############
def clearCollection(collection,force=False):
############
    '''
    Convenience function to delete collection
    '''
    
    if not force:
        answer=raw_input('Clear all?')

    if answer.lower().strip() in ['y','yes'] or force:
    	res=collection.remove()
        print 'Cleared %d posts' % res['n']

    if res['n']>0:
        return True
    else:
        return False

############
def putDfInMongo(df,collection,bulk=True):
############
    '''
    Takes dataframe of temperature measurements and
    inserts all rows in collection
    @bulk flag specifies if recrds are inserted individually
    (better if duplicates are present) or in one attempt
    Returns integer count of documents inserted
    '''

    success=False
    if bulk:
        success=collection.insert_many(df.to_dict('records'))
        return len(success.inserted_ids)
    else:
 	success=0
        for record in df.to_dict('records'):
	    try:
                res=collection.insert_one(record)
	    except pymongo.errors.DuplicateKeyError as e:
                res=None
		pass
	    if res:
	        success+=1
        return success

############
def jsonGenerator(files,verbose=False,offsets=None):
############
    '''
    Generator that parses list of json files and returns 
    data frame of each. Takes optional dictionary mapping
    station IDs to time zone offsets
    '''
    for f in files:
        with open(f,'r') as inFile:
            try:
                d=json.loads(inFile.read())
                temperature=map(lambda x:x['main']['temp'],d['list'])
                ids=map(lambda x:x['id'],d['list'])
                pressure=map(lambda x:x['main']['pressure'],d['list'])
                humidity=map(lambda x:x['main']['humidity'],d['list'])
                sensorTimes=map(lambda x:pd.datetime.fromtimestamp(x['dt']),d['list'])
                lats=map(lambda x:float(x['coord']['lat']),d['list'])
                longs=map(lambda x:float(x['coord']['lon']),d['list'])

                if offsets:
                    sensorTimes=[t+pd.DateOffset(seconds=offsets[i]) for t,i in zip(sensorTimes,ids)]

                assert len(temperature)==len(ids)==len(pressure)==len(humidity)==len(lats)==len(longs),\
			          'Data unequal (%d,%d,%d,%d,%d)' % (len(temperature),len(ids),len(pressure),len(humidity),len(lats),len(longs))
            	
                df=pd.DataFrame(data={'temperature':temperature,'id':ids,'humidity':humidity,'pressure':pressure,\
                        'sensorTime':sensorTimes,'lat':lats,'long':longs},index=sensorTimes)
                yield df
                # Only yield if dataframe populated successfully
            except:
                if verbose:
                    print 'Error reading %s' % f
                    print traceback.print_exc()
                logging.warning('File empty')
    	

def getWeekday(d):
    return d.strftime(format='%A')

def getMid(bbox):
    '''
    Helper function to get midpoint of a bounding box
    '''
    x=bbox[0]+((bbox[2]-bbox[0])/2)
    y=bbox[1]+((bbox[3]-bbox[1])/2)
    return (x,y)

# From http://www.johndcook.com/blog/python_longitude_latitude/
def distance(lat1, long1, lat2, long2):
    '''
    latitude is north-south
    '''
 
    # Convert latitude and longitude to 
    # spherical coordinates in radians.
    degrees_to_radians = math.pi/180.0
         
    # phi = 90 - latitude
    phi1 = (90.0 - lat1)*degrees_to_radians
    phi2 = (90.0 - lat2)*degrees_to_radians
         
    # theta = longitude
    theta1 = long1*degrees_to_radians
    theta2 = long2*degrees_to_radians
         
    # Compute spherical distance from spherical coordinates.
         
    # For two locations in spherical coordinates 
    # (1, theta, phi) and (1, theta', phi')
    # cosine( arc length ) = 
    #    sin phi sin phi' cos(theta-theta') + cos phi cos phi'
    # distance = rho * arc length
     
    cos = (math.sin(phi1)*math.sin(phi2)*math.cos(theta1 - theta2) + 
           math.cos(phi1)*math.cos(phi2))
    arc = math.acos( cos )
 
    # Remember to multiply arc by the radius of the earth 
    # in your favorite set of units to get length.
    return arc*earthRadius

###############################################################
def parseFile(f,nTowers,towers):
    '''
    Takes in path to file with source,destination,traffic as rows
    Returns mobility matrix nTowers x nTowers
    '''
    mobMatrix=np.zeros(shape=(nTowers,nTowers))
    
    with open(f,'r') as inFile:
        lines=inFile.read().split('\n')[1:]
        # Throw out header
#        print len(lines)
        
        for line in lines:
            line=line.split(',')
            origin=line[1]
            destination=line[2]
            mobMatrix[towers.index(origin)][towers.index(destination)]=int(line[3])
    diagonal=np.copy(mobMatrix.diagonal())
    np.fill_diagonal(mobMatrix,0.0)
    return mobMatrix,diagonal
