
import pandas as pd
import geopandas as gpd

import psycopg2
from sqlalchemy import create_engine

from ast import literal_eval

f = open('settings', 'r')
chk_config_str = f.read()
f.close()
c = literal_eval(chk_config_str)
db_name = str(c['db_name'])
db_username = str(c['db_username'])
db_password = str(c['db_password'])
db_location = str(c['db_location'])
db_port = str(c['db_port'])
analysis_starttime = c['analysis_starttime']
analysis_duration = c['analysis_duration']

def get_db_name():
    return db_name

def get_times():
    return analysis_starttime, analysis_duration

#DB helper functions
def append_to_db(df,tbl,db, index=False):
    df_cols = df.columns
    df_cols = [c.lower() for c in df_cols]
    df.columns = df_cols
    connstr = 'postgresql+psycopg2://'+db_username+':'+db_password+'@'+db_location+':'+db_port+'/'+db
    engine = create_engine(connstr)
    df.to_sql(tbl, engine,if_exists='append', index=index)
    return None

def droptbl(tbl,db):
    connx = psycopg2.connect(user=db_username, password=db_password, host=db_location, port= db_port, database=db)
    curx = connx.cursor()
    curx.execute('drop table "'+tbl+'"')
    connx.commit()
    connx.close()
    return None

def execsql(sql,db):
    connx = psycopg2.connect(user=db_username, password=db_password, host=db_location, port= db_port, database=db)
    curx = connx.cursor()
    connx.set_session(autocommit=True)
    curx.execute(sql)
    connx.commit()
    connx.close()
    return None

def gettbl(tbl,db):
    connstr = 'postgresql+psycopg2://'+db_username+':'+db_password+'@'+db_location+':'+db_port+'/'+db
    engine = create_engine(connstr)
    rdf = pd.read_sql('select * from "'+tbl+'"', engine)
    return rdf

def getsql(sql,db):
    connstr = 'postgresql+psycopg2://'+db_username+':'+db_password+'@'+db_location+':'+db_port+'/'+db
    engine = create_engine(connstr)
    rdf = pd.read_sql(sql, engine)
    return rdf

def getsql_postgis(sql,geom, db):
    con = psycopg2.connect(user=db_username, password=db_password, host=db_location, port= db_port, database=db)
    rdf = gpd.GeoDataFrame.from_postgis(sql, con, geom_col=geom, crs=None, index_col=None)
    con.close()
    return rdf


def recreatedb(db):
    connstr = 'postgresql+psycopg2://'+db_username+':'+db_password+'@'+db_location+':'+db_port+'/postgres'
    engine = create_engine(connstr)
    sql = "select datname from pg_catalog.pg_database where datname='"+db+"'"
    rdf = pd.read_sql(sql, engine)
    
    if len(rdf)!=0:
        
        #terminate all connections to db
        sql = "select pg_terminate_backend(pg_stat_activity.pid) from pg_stat_activity where pg_stat_activity.datname = '"+db+"' and pid <> pg_backend_pid();"
        execsql(sql,'postgres')  
        #drop db
        sql = "drop database "+db
        execsql(sql,'postgres')
        
    sql = "create database "+db+";"
    execsql(sql,'postgres')


    return None


