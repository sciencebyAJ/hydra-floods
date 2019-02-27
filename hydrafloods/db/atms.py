import os
import math
import glob
import netrc
import requests
import datetime
import subprocess
import xmltodict
import numpy as np
import pandas as pd
import xarray as xr
from osgeo import gdal,osr
import geopandas as gpd
from shapely.geometry import Polygon,LineString
from pyproj import Proj, transform
from pyresample import bilinear, geometry

from . import dbio

stname = 'obs.atms'

def ingest(tiff,dbname=None):
    if dbname:
        inpath = os.path.abspath(tiff)
        dateStr = inpath.split('.')[3]
        dt = datetime.datetime.strptime(dateStr,'%Y%m%dT%H%M')
        fname, ext = os.path.splitext(tiff)
        band = 'water_frac'
        dbio.ingest(dbname, inpath, dt, band, stname,nodata=-999)

    else:
        raise ValueError('dbname must be specified')

    return


def fetch(region,date,outdir='./',creds=None):
    if outdir[-1] != '/':
        outdir = outdir+'/'

    acct = netrc.netrc(creds)
    usr,_,pswrd = acct.hosts['https://urs.earthdata.nasa.gov']

    foy = datetime.datetime(date.year,1,1)

    jday = (date-foy).days + 1

    url = 'https://sounder.gesdisc.eosdis.nasa.gov/data/SNPP_Sounder_Level1/SNPPATMSL1B.2/{0}/{1:03d}/'.format(
    date.year,jday
    )

    r = requests.get(url)

    if r.ok:
        result = r.content.split(' '.encode())
        filtered = []
        for i in result:
            if 'href'.encode() in i:
                this = i.split('"'.encode())[1]
                if this[-4:] =='.xml'.encode():
                    filtered.append(this.decode("utf-8"))

    xmls = set([url+xml for xml in filtered])
    sdrfiles = swathFilter(region,xmls)

    fileList = []

    with requests.Session() as s:
        s.auth = (usr, pswrd)
        for sdr in sdrfiles:
            outFile = os.path.join(outdir,sdr)
            if os.path.exists(outFile) != True:
                newurl = url+sdr
                r1 = s.request('get', newurl)
                r2 = s.get(r1.url, auth=(usr, pswrd))

                with open(outFile, 'wb') as this:
                    this.write(r2.content)

            fileList.append(outFile)

    return fileList

def swathFilter(region,xmls):
    geoms = []
    sdrnames = []
    for xml in xmls:
        xmlStr = requests.get(xml).content
        data = xmltodict.parse(xmlStr)

        ptList= data["S4PAGranuleMetaDataFile"]['SpatialDomainContainer']['HorizontalSpatialDomainContainer']['GPolygon']['Boundary']['Point']

        verts = [(float(pt['PointLongitude']),float(pt['PointLatitude'])) for pt in ptList]
        verts.append(verts[0])
        x,y = list(zip(*verts))

        maxDist = max([abs(x[-1]-x[i]) for i in range(len(x)-1)])

        if maxDist < 60:
            geoms.append(Polygon(verts))
            sdrnames.append(data["S4PAGranuleMetaDataFile"]['DataGranule']['GranuleID'])

    swathGeo = gpd.GeoDataFrame(pd.DataFrame({'sdr':sdrnames,'geometry':geoms}),geometry=geoms)

    swathGeo.crs = {'init':'epsg:4326'}

    intersection = gpd.overlay(region,swathGeo,how='intersection')

    return list(intersection.sdr)


def unpack(infile):
    ds = xr.open_dataset(infile)

    outEpsg = 3857

    outProj = Proj(init='epsg:{0}'.format(outEpsg))
    inProj = Proj(init='epsg:4326')

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(outEpsg)

    lons,lats = ds.lon.values,ds.lat.values

    xx,yy = transform(inProj,outProj,lons,lats)
    minx,miny = transform(inProj,outProj,-180,-86)
    maxx,maxy = transform(inProj,outProj,180,86)
    res = 14000

    eastings = np.arange(round(minx),round(maxx),res)
    northings = np.arange(round(miny),round(maxy),res)

    ee = eastings[np.where((eastings>xx.min()) & (eastings<xx.max()))]
    nn = northings[np.where((northings>yy.min()) & (northings<yy.max()))]

    swath_def = geometry.SwathDefinition(lons=lons, lats=lats)
    area_def = geometry.AreaDefinition('mercator',
                                       'WGS 84 / Pseudo-Mercator - Projected',
                                       'mercator',
                                       {'x_0': '0.0', 'y_0': '0.0', 'lat_ts': '0.00',
                                        'lon_0': '0.00', 'proj': 'merc','k':'1.0',
                                        'datum':'WGS84','ellps': 'WGS84',
                                        'a':'6378137','b':'6378137'},
                                       ee.size, nn.size,
                                       [ee.min(), nn.min(),
                                        ee.max(), nn.max()])

    data = None

    result = bilinear.resample_bilinear(ds.land_frac.values,swath_def,area_def,
                                  radius=100000,neighbours=32, fill_value=-999)

    result[np.where(result>=0)] = np.abs(result[np.where(result>=0)] - 1) * 10000

    name,_ = os.path.splitext(infile)
    outName = name + '_waterfrac.TIF'

    yDim,xDim = result.shape
    gt = (ee.min(),res,0,nn.max(),0,-res)

    driver = gdal.GetDriverByName('GTiff')

    outDs = driver.Create(outName,xDim,yDim,1,gdal.GDT_Int16)
    outDs.SetGeoTransform(gt)
    # set to something that is not user defined
    outDs.SetProjection(srs.ExportToWkt())

    band = outDs.GetRasterBand(1)
    band.SetNoDataValue(-999)
    band.WriteArray(result)

    band = None
    outDs.FlushCache()

    # fileComps = infile.split('_')
    # timeComps = fileComps[2][1:] + fileComps[3][1:] + 'UTC'
    #
    # dt = datetime.datetime.strptime(timeComps,'%Y%m%d%H%M%S%f%Z')

    return outName

def _calibrate(data,gc,idx):
    calData = data[:,:,idx]

    BTr = np.zeros_like(calData).astype(np.uint16)
    for i in range(BTr.shape[0]):
        BTr[i,:] = (calData[i,:].astype(int) * gc[i,idx]) * 0.0001

    return BTr
