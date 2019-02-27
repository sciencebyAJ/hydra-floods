from . import landsat
from . import viirs
from . import atms
from . import dbio

from osgeo import gdal
import dask.array as da

bindex = {'landsat':['B1','B2','B3','B4','B5','B6','B7','B9','B10','B11','BQA'],
          'viirs':[''],
          'modis':[''],
          'atms':['land_frac']}


def get_array(dbname,stname,band=None,date=None,geom=None):

    db = dbio.connect(dbname)
    cur = db.cursor()

    schemaname, tablename = stname.split(".")

    # enable
    cur.execute("SET postgis.gdal_enabled_drivers = 'ENABLE_ALL'")
    cur.execute("SET postgis.enable_outdb_rasters TO True")

    vsipath = '/vsimem/from_postgis'

    sql = "select ST_AsGDALRaster(ST_Transform(ST_Union(rast),4326),'GTiff') as tiff from {0}.{1}".format(
            schemaname, tablename)

    if band or date:
        sql = sql + " where "

        if band:
            sql = sql + "band = '{0}'".format(band)

        if date:
            sql = sql + "band = '{0}'".format(band)

    print(sql)
    cur.execute(sql)

    data = cur.fetchall()[0][0]
    gdal.FileFromMemBuffer(vsipath, bytes(data))

    ds = gdal.Open(vsipath)
    band = ds.GetRasterBand(1)
    arr = da.from_array(band.ReadAsArray(),chunks=(1000,1000))
    selection = da.ma.masked_where(arr==band.GetNoDataValue(),arr)

    ds = band = None
    gdal.Unlink(vsipath)

    return selection
