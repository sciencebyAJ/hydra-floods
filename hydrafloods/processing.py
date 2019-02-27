import os
import ee
import yaml
import numpy as np
import xarray as xr
from osgeo import gdal,osr
import pysptools.eea as eea
import pysptools.abundance_maps as amp
from skimage.morphology import disk
from skimage.filters import threshold_otsu, rank

import rastersmith as rs

from . import geeutils, downscale

class Sentinel1(object):
    def __init__(self,):
        return

    @staticmethod
    def getFloodMap(gr,time_start,time_end,
                    canny_threshold=7,    # threshold for canny edge detection
                    canny_sigma=1,        # sigma value for gaussian filter
                    canny_lt=7,           # lower threshold for canny detection
                    smoothing=100,        # amount of smoothing in meters
                    connected_pixels=200, # maximum size of the neighborhood in pixels
                    edge_length=50,       # minimum length of edges from canny detection
                    smooth_edges=100,
                    ):

        geom = ee.Geometry.Rectangle([gr.west,gr.south,gr.east,gr.north])

        mapResult = geeutils.s1WaterMap(geom,time_start,time_end,canny_threshold,
                                        canny_sigma,canny_lt,smoothing,
                                        connected_pixels,edge_length,
                                        smooth_edges)

        return mapResult


class Atms(object):
    def __init__(self):
        return

    @staticmethod
    def maskClouds(ds,threshold=-20):
        rain = ds.sel(band='C16').astype(np.float) - ds.sel(band='C1').astype(np.float)

        cloudMask = rain > threshold

        return ds.raster.updateMask(cloudMask)

    @classmethod
    def getWaterFraction(cls,ds,cloudThresh=-20,constrain=True,maskClouds=True):

        if maskClouds:
            atmsNoClouds = cls.maskClouds(ds,threshold=cloudThresh)
        else:
            atmsNoClouds = ds.copy()

        dBtr = atmsNoClouds.sel(band='C4').astype(np.float) - atmsNoClouds.sel(band='C3').astype(np.float)
        dBtr.coords['band'] = 'dBtr'

        channels = xr.concat([atmsNoClouds.sel(band=['C3','C4','C16']).isel(time=0,z=0),
                              dBtr.isel(time=0,z=0)],dim='band')
        arr = channels.values
        arr[np.isnan(arr)] = -9999

        nClasses = 3
        nfindr = eea.NFINDR()
        U = nfindr.extract(arr, nClasses, maxit=100, normalize=True, ATGP_init=True)

        drop = np.argmin(list(map(lambda x:U[x,:].mean(),range(nClasses))))
        waterIdx = np.argmin(list(map(lambda x:np.delete(U,drop,axis=1)[x,:],range(nClasses-2))))

        if waterIdx == 0:
            bandList = ['water','land','mask']
        else:
            bandList = ['land','water','mask']

        nnls = amp.NNLS()
        amaps = nnls.map(arr, U, normalize=True)

        drop = np.argmin(list(map(lambda x:amaps[:,:,x].mean(),range(amaps.shape[2]))))

        unmixed = np.delete(amaps,drop,axis=2)

        unmixed[unmixed==0] = np.nan

        scaled = np.zeros_like(unmixed)
        for i in range(scaled.shape[2]):
            summed = unmixed[:,:,i]/unmixed.sum(axis=2)
            scaled[:,:,i] = (summed - np.nanmin(summed)) / (np.nanmax(summed) - np.nanmin(summed))

        scaled[scaled<0] = 0

        fWater = atmsNoClouds.sel(band=['C1','C2','mask']).copy()
        fWater[:,:,0,:2,0] = scaled[:,:,:]
        fWater.coords['band'] = bandList

        return fWater.raster.updateMask(atmsNoClouds.sel(band='mask'))



class Landsat8(object):
    def __init__():
        return

    @staticmethod
    def getWaterMask(ds):
        arr = ds.isel(time=0,z=0,band=0).values
        global_otsu = threshold_otsu(arr[~np.isnan(arr)])
        waterMask = ds >= global_otsu

        return waterMask.where(waterMask>0)


class Viirs(object):
    def __init__(self):
        return

    @staticmethod
    def getWaterMask(ds):
        arr = ds.isel(time=0,z=0,band=0).values
        global_otsu = threshold_otsu(arr[~np.isnan(arr)])
        waterMask = ds >= global_otsu

        return waterMask.where(waterMask>0)


class Modis(Viirs):
    def __init__():
        core.Viirs.__init__(self)
        return


class Sentinel2(object):
    def __init__():
        return


class Mosaic(object):
    def __init__():
        return

    @staticmethod
    def alignCRS(dsList,et=0.125,resampling='near',epsg=4326):
        # Define target SRS
        dst_srs = osr.SpatialReference()
        dst_srs.ImportFromEPSG(epsg)
        dst_wkt = dst_srs.ExportToWkt()

        dst_epsg = 'EPSG:{}'.format(epsg)
        appendage = dst_epsg.replace(':','')

        warp_options = gdal.WarpOptions(dstSRS=dst_epsg,
                                        resampleAlg=resampling,
                                        errorThreshold=et,
                                        )

        warped = []

        for src_ds in dsList:
            name,ext = os.path.splitext(src_ds)
            lastInfo = name.split('_')[-1]
            if lastInfo != appendage:
                proj_ds = name+'_'+appendage+ext
                if os.path.exists(proj_ds) == False:
                    out_ds = gdal.Warp(proj_ds,src_ds,options=warp_options)
                warped.append(os.path.abspath(proj_ds))

        print(warped)

        return warped

    @staticmethod
    def generateMosaic(dsList,outFile,**kwargs):

        vrt_options = gdal.BuildVRTOptions(resampleAlg='cubic',
                                           srcNodata=0,
                                           VRTNodata=0,
                                           separate=True)
        gdal.BuildVRT(outFile, dsList, options=vrt_options)

        fields = {}

        for i,ds in enumerate(dsList):
            dsDir = os.path.dirname(os.path.abspath(ds))
            mtlFile = [os.path.join(dsDir,j) for j in os.listdir(dsDir) if j[-7:] == 'MTL.txt' ][0]
            metadata = rs.Landsat._parseMetadata(mtlFile)

            fields['B{}'.format(i+1)] = metadata

        outName, _ = os.path.splitext(outFile)
        with open(outName+'.yml','w') as f:
            yaml.dump(fields, f)

        return

    @staticmethod
    def open(vrtFile,metadata):

        return
