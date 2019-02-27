import os
import math
import glob
import datetime
import subprocess
import numpy as np
from osgeo import gdal,osr
from scipy import ndimage
import geopandas as gpd
from pyproj import Proj, transform


def ingest(file,date):

    return

def fetch(date,h,v,outdir='./',creds=None,product=None):
    """Function to download VIIRS NRT data for specified time and tile

    Args:
        date (datetime.datetime): Datetime object specifying which date the data of interest was acquired.
        h (int): horizontal tile grid to fetch
        v (int): vertical tile grid to fetch
        outdir (str, optional): out directory to dump retrieved data to
        default = './' or current working directory
        creds (str, optional): path to .netrc file with NASA EarthData login in credentials
        default = None

    Returns:
        None
    """

    if outdir[-1] != '/':
        outdir = outdir+'/'

    acct = netrc.netrc(creds)
    usr,_,pswrd = acct.hosts['https://urs.earthdata.nasa.gov']

    today = datetime.datetime.now()

    if outdir[-1] != '/':
        outdir = outdir+'/'

    basename = '{0}.A{1}{2:03d}.h{3:02d}v{4:02d}.001.h5'

    if (today - date).days > 8:
        yr = date.year
        dt = (date-datetime.datetime(yr,1,1)).days + 1
        url = 'https://e4ftl01.cr.usgs.gov/DP102/VIIRS/{0}.001/{1}.{2:02d}.{3:02d}/'\
                .format(product,yr,date.month,date.day)
        with requests.Session() as s:
            s.auth = (usr, pswrd)

            r1 = s.request('get', url)
            r = s.get(r1.url, auth=(usr, pswrd))

            if r.ok:
                result = r.content.split(' '.encode())
                filtered = []
                for i in result:
                    if 'href'.encode() in i:
                        this = i.split('"'.encode())[1]
                        if this[-3:] =='.h5'.encode():
                            filtered.append(this.decode("utf-8"))

                for f in filtered:
                    if 'h{:02d}'.format(h) in f\
                    and 'v{:02d}'.format(v)  in f:
                        filename = basename.format(product,yr,dt,h,v)
                        outFile = outdir + filename
                        if os.path.exists(outFile) != True:
                            newurl = url+f
                            r3 = s.request('get', newurl)
                            r4 = s.get(r3.url, auth=(usr, pswrd))

                            with open(outFile, 'wb') as this:
                                this.write(r4.content) # Say

    else:
        yr = date.year
        dt = (date-datetime.datetime(yr,1,1)).days + 1

        url = 'https://nrt3.modaps.eosdis.nasa.gov/api/v2/content/archives/allData/5000/{0}/Recent/'.format(product+'_NRT')
        filename = basename.format(product+'_NRT',yr,dt,h,v)
        fileUrl = url + filename

        outFile = outdir + filename
        if os.path.exists(outFile) != True:
            with requests.Session() as s:
                s.auth = (usr, pswrd)

                r1 = s.request('get', fileUrl)
                r2 = s.get(r1.url, auth=(usr, pswrd))

                with open(outFile, 'wb') as f:
                   f.write(r2.content)

    return outFile

def unpack(infile):
    tree = '//HDFEOS/GRIDS/VNP_Grid_{}_2D/Data_Fields/'
    field = 'SurfReflect_{0}{1}_1'
    base = 'HDF5:"{0}":{1}{2}'

    # sinuWkt = '''PROJCS["Sphere_Sinusoidal",GEOGCS["GCS_Sphere",DATUM["D_Sphere",SPHEROID["Sphere",6371000.0,0.0]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Sinusoidal"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",0.0],UNIT["Meter",1.0]]'''


    proj = '+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs'
    outProj = Proj(proj)
    inProj = Proj(init='epsg:4326')

    srs = osr.SpatialReference()
    srs.ImportFromProj4(outProj.definition_string())

    m = [i for i in range(12) if i not in [0,6,9]]
    i = [i for i in range(1,4)]
    bands = [m,i]
    flatBands = [item for sublist in bands for item in sublist]

    res = ['1km','500m']
    mode = ['M','I']

    band = gdal.Open(base.format(infile,tree.format('1km'),field.format('QF',1)))
    metadata = band.GetMetadata()
    # print(metadata)
    cloudQA = _extractBits(band.ReadAsArray(),2,3)
    hiresCloudQA = ndimage.zoom(cloudQA,2,order=0)
    band = None

    band = gdal.Open(base.format(infile,tree.format('1km'),field.format('QF',2)))
    shadowQA = _extractBits(band.ReadAsArray(),3,3)
    hiresShadowQA = ndimage.zoom(shadowQA,2,order=0)

    qa = ~(hiresCloudQA>0)&(hiresShadowQA<1)

    ringLatitude = metadata['GRingLatitude'].split(' ')[:-1]
    ringLongitude = metadata['GRingLongitude'].split(' ')[:-1]

    ll,ul,ur,lr = [transform(inProj,outProj,float(ringLongitude[i]),float(ringLatitude[i])) for i in range(len(ringLatitude))]
    iniX,iniY = ul[0],ul[1]
    print(ll,ul,ur,lr)

    bandNames = ['{0}{1}'.format(mode[i],bands[i][j]) \
                    for i in range(len(res)) \
                    for j in range(len(bands[i]))
                ]

    subdata = [[infile,res[i],mode[i],bands[i][j]] \
                    for i in range(len(res)) \
                    for j in range(len(bands[i]))
              ]

    driver = gdal.GetDriverByName('GTiff')
    res = float(metadata['CharacteristicBinSize500M'])
    yskew = math.sin(_getSkew(ur,ul)) * res
    xskew = math.cos(_getSkew(ul,ll)) * res
    print(xskew,yskew)
    yDim, xDim = qa.shape
    gt = (10007554.677,res,-0.49650400890037416,2223901.039333,yskew,-res)
    print(gt)

    name,_ = os.path.splitext(infile)
    outtiffs = []

    dlist = []

    for i,s in enumerate(subdata):
        outName = name + '_{}.TIF'.format(bandNames[i])
        print(outName)

        infile, r, m, b = s

        subdataset = base.format(infile,tree.format(r),field.format(m,b))

        band = gdal.Open(subdataset)
        if m == 'M':
            data = ndimage.zoom(band.ReadAsArray(),2,order=0)

        else:
            data = np.array(band.ReadAsArray())
        band = None

        data[np.where(data<0)] = -999

        outDs = driver.Create(outName,xDim,yDim,1,gdal.GDT_Int16)
        outDs.SetGeoTransform(gt)
        outDs.SetProjection(srs.ExportToWkt())

        band = outDs.GetRasterBand(1)
        band.SetNoDataValue(-999)
        band.WriteArray(data)

        band = None
        outDs.FlushCache()

        outtiffs.append(outName)

    outName = name + '_{}.TIF'.format('QF')
    outDs = driver.Create(outName,xDim,yDim,1,gdal.GDT_Int16)
    outDs.SetGeoTransform(gt)
    outDs.SetProjection(srs.ExportToWkt())

    band = outDs.GetRasterBand(1)
    band.SetNoDataValue(-999)
    band.WriteArray(data)

    band = None
    outDs.FlushCache()

    outtiffs.append(outName)

    return outtiffs

def findTiles(region, tiles):
    """Returns the tile IDs that need to be downloaded for
    a given region bounded by *region*."""

    if region is None:
        raise ValueError("No bounding box provided for study area. Aborting download!")
        ids = None
    else:
        intersection = gpd.overlay(region,tiles,how='intersection')

        if 'PATH' in intersection.columns:
            h,v = 'PATH','ROW'
        elif 'h' in intersection.columns:
            h,v = 'h','v'
        else:
            raise AttributeError('cannot parse the needed tile information from provided geopadas dataframe')

        ids = [(intersection.iloc[i][h],intersection.iloc[i][v]) for i in range(len(intersection))]

    return ids

def _extractBits(image,start,end):
        """Helper function to convert Quality Assurance band bit information to flag values

        Args:
            image (ndarray): Quality assurance image as a numpy array
            start (int): Bit position to start value conversion
            end (int): Bit position to end value conversion

        Returns:
            out (ndarray): Output quality assurance in values from bit range
        """

        pattern = 0;
        for i in range(start,end+1):
            pattern += math.pow(2, i)

        bits = image.astype(np.uint16) & int(pattern)
        out = bits >> start

        return out

def _getSkew(pt1,pt2,degrees=False):
    xoff = pt1[0] - pt2[0]
    yoff = pt1[1] - pt2[1]
    result = math.atan2(yoff,xoff)
    if degrees:
        result = math.degrees(result)
    return result
