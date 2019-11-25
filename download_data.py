
import os
import sys
sys.path.append(os.getcwd())
import helperfunctions as hf

import urllib
import time
import datetime
import os
from xml.etree import cElementTree as ET

import pandas as pd

import psycopg2
from sqlalchemy import create_engine

db = hf.get_db_name()
#drop and recreate db
hf.recreatedb(db)

sql = 'create extension postgis;'
hf.execsql(sql,db)

start_time, duration = hf.get_times()
start_time = datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M') 
end_time = start_time+datetime.timedelta(hours = duration)

cur_time = datetime.datetime.now()
waittime_sec = (start_time-cur_time).total_seconds()

print('start time: ',start_time)
print('end time: ', end_time)
print('wait time: ', waittime_sec)

time.sleep(waittime_sec)


def record_routes(data_xml):

    ttc_route_dict = []
    ttc_route_stop_dict = []
    ttc_dir_dict = []
    ttc_dir_stop_dict = []
    ttc_route_path_dict = []

    ttc_dir_stop = []
    ttc_route_stop = []
    ttc_route_path = []
    ttc_dir = []
    cnt_path = 0





    for child in data_xml:
        if child.tag == 'route':
            route_tag = child.attrib.get('tag')
            ttc_route_dict.append(child.attrib)
            for lv1child in child:
                if lv1child.tag == 'stop':
                    ttc_route_stop_dict.append(lv1child.attrib)
                if lv1child.tag == 'path':
                    cnt_path=cnt_path+1
                    for lv2child in lv1child:
                        tmp_attrib = lv2child.attrib
                        tmp_attrib['pathid']=cnt_path
                        ttc_route_path_dict.append(lv2child.attrib)

                if lv1child.tag == 'direction':
                    ttc_dir_dict.append(lv1child.attrib)
                    dir_tag = lv1child.attrib.get('tag')
                    for lv2child in lv1child:
                        ttc_dir_stop_dict.append(lv2child.attrib)
                    ttc_dir_stop_tmpdf = pd.DataFrame(ttc_dir_stop_dict)
                    ttc_dir_stop_tmpdf['directiontag'] = dir_tag
                    ttc_dir_stop_tmpdf['routetag'] = route_tag
                    ttc_dir_stop_tmpdf = ttc_dir_stop_tmpdf.reset_index()
                    ttc_dir_stop_tmpdf = ttc_dir_stop_tmpdf.rename(columns={'index':'oid'})
                    if len(ttc_dir_stop) == 0:
                        ttc_dir_stop = ttc_dir_stop_tmpdf
                    else:
                        ttc_dir_stop = pd.concat([ttc_dir_stop, ttc_dir_stop_tmpdf])
                    ttc_dir_stop_dict=[]

            ttc_dir_tmpdf = pd.DataFrame(ttc_dir_dict)
            ttc_dir_tmpdf['routetag'] = route_tag

            if len(ttc_dir) == 0:
                ttc_dir = ttc_dir_tmpdf
            else:
                ttc_dir = pd.concat([ttc_dir, ttc_dir_tmpdf])

            ttc_route_stop_tmpdf =  pd.DataFrame(ttc_route_stop_dict)
            ttc_route_stop_tmpdf['routetag'] = route_tag
            ttc_route_stop_tmpdf = ttc_route_stop_tmpdf.reset_index()
            ttc_route_stop_tmpdf = ttc_route_stop_tmpdf.rename(columns={'index':'oid'})

            if len(ttc_route_stop) == 0 :
                ttc_route_stop = ttc_route_stop_tmpdf
            else:
                ttc_route_stop = pd.concat([ttc_route_stop, ttc_route_stop_tmpdf])

            ttc_route_path_tmpdf =  pd.DataFrame(ttc_route_path_dict)
            ttc_route_path_tmpdf['routetag'] = route_tag
            ttc_route_path_tmpdf = ttc_route_path_tmpdf.reset_index()
            ttc_route_path_tmpdf = ttc_route_path_tmpdf.rename(columns={'index':'oid'})
            if len(ttc_route_path) == 0 :
                ttc_route_path = ttc_route_path_tmpdf
            else:
                ttc_route_path = pd.concat([ttc_route_path, ttc_route_path_tmpdf])

    ttc_route = pd.DataFrame(ttc_route_dict)

    hf.append_to_db(ttc_route,'tbl_ttc_route' , db,index=False)
    hf.append_to_db(ttc_route_path,'tbl_ttc_route_path', db ,index=False)
    hf.append_to_db(ttc_route_stop,'tbl_ttc_route_stop', db ,index=False)
    hf.append_to_db(ttc_dir,'tbl_ttc_dir', db ,index=False)
    hf.append_to_db(ttc_dir_stop,'tbl_ttc_dir_stop', db ,index=False)


def writelogtodb(logdf):
    
    tbl_name = 'tbl_ttc_location_log'

    #remove any columns from the df that aren't in the expected list of columns
    collist=['dirTag', 'heading','id','lat','lon','predictable','routeTag','secsSinceReport','speedKmHr','timestamp']
    logdf = logdf[logdf.columns.intersection(collist)].copy()


    #append the dataframe to the database
    hf.append_to_db(logdf,tbl_name, db, index=False)
    
t = '0'

url_loc = 'http://webservices.nextbus.com/service/publicXMLFeed?command=vehicleLocations&a=ttc&t='

while cur_time<end_time:
    try:
        data = urllib.request.urlopen(url_loc+t)
        
        data_b = data.read()
        data_s = data_b.decode('utf-8')
        data_xml = ET.fromstring(data_s)

        c=[]
        ttc_loc=[]

        for child in data_xml:
            if child.tag == 'lastTime':
                t = child.attrib.get('time')
            else:
                c.append(child.attrib)

            ttc_loc = pd.DataFrame(c)
            ttc_loc['timestamp']=t

        writelogtodb(ttc_loc)
        time.sleep(30)
    
    except urllib.error.HTTPError as e:
        print(e.code)
        print(e.read())
        time.sleep(30)
    except urllib.error.URLError as e:
        print(e.reason)
        
        time.sleep(30)
    cur_time = datetime.datetime.now()

print('downloading routes')        
#download routes
url_routelist = 'http://webservices.nextbus.com/service/publicXMLFeed?command=routeList&a=ttc'
data = urllib.request.urlopen(url_routelist)
data_b = data.read()
data_s = data_b.decode('utf-8')
oxml = ET.fromstring(data_s)

#this can take a while as we wait 30 seconds between each request for a TTC route
for child in oxml:
    if child.tag == 'route':
        rt = child.attrib.get('tag')
        url_route = 'http://webservices.nextbus.com/service/publicXMLFeed?command=routeConfig&a=ttc&r='+rt
        data = urllib.request.urlopen(url_route)
        data_b = data.read()
        data_s = data_b.decode('utf-8')
        
        record_routes(ET.fromstring(data_s))
        time.sleep(10)   

        
sql = """create index inx_tbl_ttc_location_log_id on tbl_ttc_location_log (id);

create or replace function fn_epoch_to_dt(bigint)
    returns timestamp
    language 'sql'
    cost 100
as $BODY$
select (timestamp with Time Zone 'epoch' + $1 * interval '1 second')::timestamp without time zone;
$BODY$;"""

hf.execsql(sql,db)

