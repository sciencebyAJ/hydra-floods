
import numpy as np
from osgeo import gdal, osr
import subprocess
import random
import psycopg2 as pg
import string
# import rpath
import sys
import logging


def connect(dbname):
    """Connect to database *dbname*."""
    try:
        db = pg.connect(database=dbname)
    except pg.OperationalError:
        db = None
        try:
            db = pg.connect(database=dbname, host="/tmp/")
        except:
            log.error("Cannot connect to database {0}. Please restart it by running \n {1}/pg_ctl -D {2}/postgres restart".format(
                dbname, rpath.bins, rpath.data))
            sys.exit()
    return db


def tableExists(dbname, schemaname, tablename):
    """Check if table exists in the database."""
    db = connect(dbname)
    cur = db.cursor()
    cur.execute("select * from information_schema.tables where table_schema='{0}' and table_name='{1}'".format(schemaname, tablename))
    table_exists = bool(cur.rowcount)
    cur.close()
    db.close()
    return table_exists


def schemaExists(dbname, schemaname):
    """Check if schema exists in database."""
    db = connect(dbname)
    cur = db.cursor()
    cur.execute("select * from information_schema.schemata where schema_name='{0}'".format(schemaname))
    schema_exists = bool(cur.rowcount)
    cur.close()
    db.close()
    return schema_exists


def _createRasterTable(dbname, stname):
    """Create table *stname* holding rasters in database *dbname*."""
    db = connect(dbname)
    cur = db.cursor()
    cur.execute(
        "create table {0} (rid serial primary key, rast raster, fdate date not null)".format(stname))
    db.commit()
    cur.close()
    db.close()

    return


def _createDateIndex(dbname, schemaname, tablename):
    """Create table index based on date column."""
    db = connect(dbname)
    cur = db.cursor()
    cur.execute("create index {1}_t on {0}.{1}(fdate)".format(schemaname, tablename))
    db.commit()
    cur.close()
    db.close()

    return

def deleteRasters(dbname, tablename, dt, band, squery=""):
    """If date already exists delete associated rasters before
    ingesting, and optionally constrain with subquery."""
    log = logging.getLogger(__name__)
    db = connect(dbname)
    cur = db.cursor()
    # get date string
    dstr = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    sql = "select * from {0} where ftime='{1}' and band='{2}'".format(tablename, dstr, band)
    cur.execute(sql)
    if bool(cur.rowcount):
        log.warning("Overwriting raster in {0} table for {1}".format(tablename, dstr))
        cur.execute("delete from {0} where ftime='{1}' and band='{2}' {3}".format(tablename, dstr,band, squery))
        db.commit()
    cur.close()
    db.close()

def addBand(dbname,tablename,dt):

    return


def ingest(dbname, filename, dt, band, stname, nodata=0, tilesize=(100,100), resample=False, overwrite=True):
    """Imports Geotif *filename* into database *db*."""
    log = logging.getLogger(__name__)
    db = connect(dbname)
    cur = db.cursor()
    # import temporary table
    temptable = ''.join(random.SystemRandom().choice(
        string.ascii_letters) for _ in range(8)).lower()
    cmd = "raster2pgsql -d -N {0} -t {1}x{2} {3} {5} | psql -d {4}".format(
          nodata,tilesize[0],tilesize[1],filename, dbname, temptable)
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out, err = proc.communicate()
    log.debug(out)
    cur.execute("alter table {0} add column ftime timestamp".format(temptable))
    cur.execute("alter table {0} add column band text".format(temptable))
    # get date string
    dstr = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    cur.execute(
        "update {2} set ftime = timestamp '{0}', band = text '{1}' ".format(
        dstr, band, temptable))
    # check if table exists
    schemaname, tablename = stname.split(".")
    if not schemaExists(dbname, schemaname):
        cur.execute("create schema {0}".format(schemaname))
        db.commit()
    if not tableExists(dbname, schemaname, tablename):
        _createRasterTable(dbname, stname)
        _createDateIndex(dbname, schemaname, tablename)
    # check if date already exists and delete it before ingesting
    if overwrite:
        deleteRasters(dbname, "{0}.{1}".format(schemaname, tablename), dt, band)
    # create tiles from imported raster and insert into table
    cur.execute("insert into {0}.{1} (ftime,band,rast) select ftime,band,rast from {2}".format(
        schemaname, tablename, temptable))
    db.commit()
    # create materialized views for resampled rasters
    if resample:
        # need to update functionality
        log.info("Creating resampled table for {0}.{1}".format(schemaname, tablename))
        createResampledTables(dbname, schemaname, tablename, dt, tilesize, overwrite)
    # else:
    #     log.info("Creating row for {0}.{1}".format(schemaname, tablename))
    #     cur.execute("insert into {0}.{1} (ftime,band,rast) select ftime,band,rast from {2}".format(
    #         schemaname, tablename, temptable))
    # delete temporary table
    cur.execute("drop table {0}".format(temptable))
    db.commit()
    log.info("Imported {0} in {1}".format(dt.strftime("%Y-%m-%d"), stname))
    cur.close()
    db.close()

    return
