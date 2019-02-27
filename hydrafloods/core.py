from . import db
import numpy as np
from osgeo import gdal
from pyproj import Proj, transform
import struct
import dask
import dask.array as da


class Collection(object):
    def __init__(self,dbname,stname):
        self.dbname = dbname
        self.stname = stname

        self.schemaname, self.tablename = stname.split(".")

        self.sqlstmt = 'WITH'

        self.bands = db.bindex[self.tablename]

        self.selection=self.spaceFilter=self.timeFilter=self.rescale = None
        self.reduction = 'last'

        self.chain = {}

        return


    def merge(self,how):
        self.reduction = how

        return


    def filterBounds(self,geom,epsg='4326'):
        database = db.dbio.connect(self.dbname)
        cur = database.cursor()

        inProj = Proj(init='epsg:{}'.format(epsg))

        cur.execute("SELECT rid from obs.landsat")
        ids = cur.fetchone()[0]

        cur.execute('SELECT ST_SRID(rast) As srid FROM {0}.{1} WHERE rid={2}'.format(
            self.schemaname,self.tablename,ids
        ))
        srid = cur.fetchone()[0]

        refProj = Proj(init='epsg:{}'.format(srid))


        if type(geom) in [tuple,list]:
            if len(geom) == 2:
                if type(geom[0]) != list:
                    if inProj.srs != refProj.srs:
                        x,y = transform(inProj,refProj,geom[0],geom[1])
                    else:
                        x,y = geom[0],geom[1]
                    sqlGeo = "ST_GeomFromText('Point({0} {1})', {2})".format(x,y,srid)

            else:
                pts = ""
                for pt in geom:
                    if inProj.srs != refProj.srs:
                        x,y = transform(inProj,refProj,pt[0],pt[1])
                    else:
                        x,y = pt[0],pt[1]
                    pts = pts + "{0} {1}, ".format(x,y)
                sqlGeo = "ST_GeomFromText('Polygon({0})', {1})".format(pts,srid)

            sql = "ST_Intersection(rast,{})".format(sqlGeo)

            self.chain['intersection'] = sql

            self.spaceFilter = sql

        cur.close()
        database.close()

        return


    def filterTime(self,iniDate,endDate):
        inistr = iniDate.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        endstr = endDate.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        sql = "ftime >= timestamp '{0}' and ftime < timestamp '{1}'".format(inistr,endstr)

        self.timeFilter = sql

        return

    def select(self,bands):
        if type(bands) == list:
            for i,b in enumerate(bands):
                if i == 0:
                    sql = "band = '{}'".format(b)
                else:
                    sql = sql + "and band = '{}'".format(b)
        else:
            sql = "band = '{}'".format(bands)

        self.selection = sql
        self.bands = bands

        return

    def resample(self,scale,how='NearestNeighbor'):
        sql = "ST_Rescale(rast, {0}, algorithm={1})".format(scale,how)
        self.rescale = sql

        self.chain['rescale'] = sql

        return

    @property
    def gt_matrix(self):
        # ST_GeoReference(rast)


        return gt

    def request(self):
        return

    @property
    def values(self):

        database = db.dbio.connect(self.dbname)
        cur = database.cursor()

        # for gdal exports to enable
        cur.execute("SET postgis.gdal_enabled_drivers = 'ENABLE_ALL'")
        cur.execute("SET postgis.enable_outdb_rasters TO True")

        if len(self.chain) > 0:
            keys = list(self.chain.keys())
            if ('intersection' in keys):
                chain = self.chain['intersection']
                if ('rescale' in keys):
                    chain = self.chain['rescale'].replace('rast',chain)

            elif 'rescale' in keys:
                chain = self.chain['rescale']

            sql = "select ST_AsBinary(ST_Transform(ST_Union({0}),4326)) as tiff from {2}.{3}".format(
                    chain,self.reduction,self.schemaname,self.tablename)
        else:
            sql = "select ST_AsBinary(ST_Transform(ST_Union(rast),4326)) as tiff from {1}.{2}".format(
                    self.reduction,self.schemaname,self.tablename)

        sql2 = sql

        if self.selection and self.timeFilter:
            sql2 += " where {0} and {1}".format(
            self.selection, self.timeFilter)

        elif self.selection or self.timeFilter:
            sql2 += " where"

            if self.selection:
                sql2 += ' {0}'.format(self.selection)

            if self.timeFilter:
                sql2 += ' {0}'.format(self.timeFilter)

        else:
            pass

        print(sql2)
        cur.execute(sql2)
        data = cur.fetchall()[0][0]

        h = wkbHeader(data)
        values = wkbImage(data)

        cur.close()
        database.close()

        return values


# Function to decypher the WKB header
def wkbHeader(raw):
    # See http://trac.osgeo.org/postgis/browser/trunk/raster/doc/RFC2-WellKnownBinaryFormat

    header = {}

    header['endianess'] = struct.unpack('B', raw[0])[0]
    header['version'] = struct.unpack('H', raw[1:3])[0]
    header['nbands'] = struct.unpack('H', raw[3:5])[0]
    header['scaleX'] = struct.unpack('d', raw[5:13])[0]
    header['scaleY'] = struct.unpack('d', raw[13:21])[0]
    header['ipX'] = struct.unpack('d', raw[21:29])[0]
    header['ipY'] = struct.unpack('d', raw[29:37])[0]
    header['skewX'] = struct.unpack('d', raw[37:45])[0]
    header['skewY'] = struct.unpack('d', raw[45:53])[0]
    header['srid'] = struct.unpack('i', raw[53:57])[0]
    header['width'] = struct.unpack('H', raw[57:59])[0]
    header['height'] = struct.unpack('H', raw[59:61])[0]

    return header

 # Function to decypher the WKB raster data
def wkbImage(raw):
    h = wkbHeader(raw)
    img = [] # array to store image bands
    offset = 61 # header raw length in bytes
    for i in range(h['nbands']):
        # Determine pixtype for this band
        pixtype = struct.unpack('B', raw[offset])[0]>>4
        print(pixtype)
        band = np.frombuffer(raw, dtype='int16', count=h['width']*h['height'], offset=offset+1)
        img.append((np.reshape(band, ((h['height'], h['width'])))))
        offset = offset + 2 + h['width']*h['height']
        # to do: handle other data types
    return img
