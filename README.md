# Summary

Python scripts to ingest Open Weather Map data into MongoDB

``ingest_all.py`` takes in all files and creates DB from scratch
``ingest_incremental.py`` takes in new files and adds to DB excluding duplicates measurements  

# Range Query

``db.clean.find({sensorTime:{$lt:ISODate("2016-02-25T00:00:00.000Z")}})``   

# TODO

- Ingest incrementally, searching for duplicates with each insert  
- ~~Create separate collection with coords, names and IDs of towers on first ingest~~  
- Add in time zones of towers ([reverse geo-coding?](https://developers.google.com/maps/documentation/geocoding/intro#ReverseGeocoding) or [time-zone API](https://developers.google.com/maps/documentation/timezone/intro)) 
- Move/rename files after processing 
- When ingesting incrementally, update downsampled values in ``cleanDaily`` by understanding which time periods are updated   
- Add a column to ``cleanDaily`` that shows how many measurements average is based upon? i.e. ``df.n=df.resample('12h',how='count')``
