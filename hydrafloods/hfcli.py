import os
import fire
import glob
import yaml
import warnings
# import xarray as xr
import geopandas as gpd
# import rastersmith as rs

from . import fetch
from . import utils
# from . import downscale
# from . import processing as proc
from . import db


class hydrafloods(object):
    def __init__(self,configuration=None):
        if configuration:
            self.filePath = os.path.dirname(os.path.abspath(__file__))
            yamlFile = configuration

            with open(yamlFile,'r') as stream:
                try:
                    struct = yaml.load(stream)
                except yaml.YAMLError as exc:
                    print(exc)

            conf = struct['configuration']
            self.conf = conf
            ftch = struct['download']
            self.ftch = ftch
            prcs = struct['process']
            self.prcs = prcs

            confKeys = list(conf.keys())
            ftchKeys = list(ftch.keys())
            prcsKeys = list(prcs.keys())

            if 'name' in confKeys:
                self.name = conf['name']
            else:
                raise AttributeError('provided yaml file does not have a name parameter in configuration')

            if 'region' in confKeys:
                self.region = gpd.read_file(conf['region'])
            elif 'country' in confKeys:
                world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
                country = world[world.name == conf['country']]
                if len(country) >= 1:
                    self.region = country
                else:
                    raise ValueError('could not parse selected country from world shapefile')

            elif 'boundingbox' in confKeys:
                from shapely import geometry
                self.region = gpd.GeoDataFrame(pd.DataFrame({'id':[0],'geometry':[geometry.box(*conf['boundingbox'])]}))
            else:
                raise AttributeError('provided yaml file does not have a specified region in configuration')

            if 'db' in confKeys:
                self.db = conf['db']
            else:
                raise AttributeError('provided yaml file does not have a specified database name in configuration')

            if 'credentials' in ftchKeys:
                self.credentials = ftch['credentials']
            else:
                self.credentials = None

            if 'outdir' in ftchKeys:
                self.ftchOut = ftch['outdir']
            else:
                warning.warn("output dir for downloading could not be parsed from provided yaml file, setting output dir to current dir",
                            UserWarning)
                self.ftchOut = './'
            if 'viirs' in ftchKeys:
                self.viirsFetch = ftch['viirs']
            if 'modis' in ftchKeys:
                self.modisFetch = ftch['modis']

            if 'atms' in prcsKeys:
                self.atmsParams = prcs['atms']
            if 'landsat' in prcsKeys:
                self.landsatParams = prcs['landsat']
            if 'viirs' in prcsKeys:
                self.viirsParams = prcs['viirs']

            if 'outdir' in prcsKeys:
                self.prcsOut = prcs['outdir']
            else:
                warning.warn("output dir for processing could not be parsed from provided yaml file, setting output dir to current dir",
                            UserWarning)
                self.prcsOut = './'

        else:
            raise ValueError('yamlfile configuration argument must be specified')

        return

    def ingest(self, product, date,dbname='hydradb'):
        date = utils.decode_date(date)

        if product in ['atms','viirs','landsat','modis']:
            dateDir = os.path.join(self.ftchOut,date.strftime('%Y%m%d'))
            if os.path.exists(dateDir) == False:
                os.mkdir(dateDir)

            prodDir = os.path.join(dateDir,product)
            if os.path.exists(prodDir) == False:
                os.mkdir(prodDir)

            files = []
            if product == 'landsat':
                tileShp = gpd.read_file(os.path.join(self.filePath,'data/landsat_wrs2.geojson'))
                downTiles = fetch.findTiles(self.region,tileShp)
                bools = [False if i != 0 else True for i in range(len(downTiles))]
                args = list(zip(*[downTiles,bools]))

                for i,tile in enumerate(downTiles):
                    p,r = tile
                    files = db.landsat.fetch(date,p,r,outdir=prodDir,updateScenes=bools[i],
                                        maxClouds=50)
                    if files:
                        db.landsat.ingest(files,dbname=self.db)

                # files = [f for f in files if f is not None]

            elif product == 'viirs':
                product = self.viirsFetch['product']
                tileShp = gpd.read_file(os.path.join(self.filePath,'data/viirs_sinu.geojson'))
                downTiles = db.viirs.findTiles(self.region,tileShp)

                files = list(map(lambda x: fetch.viirs(date,x[0],x[1],prodDir,creds=self.credentials,
                                                       product=product)
                                ,downTiles)
                            )

            elif product == 'modis':
                product,platform = self.modisFetch['product'],self.modisFetch['platform']
                tileShp = gpd.read_file(os.path.join(self.filePath,'data/viirs_sinu.geojson'))
                downTiles = fetch.findTiles(self.region,tileShp)
                print(downTiles)

                files = list(map(lambda x: fetch.modis(date,x[0],x[1],prodDir,creds=self.credentials,
                                                       product=product,platform=platform)
                                ,downTiles)
                            )

            elif product == 'atms':
                h5files = db.atms.fetch(self.region,date,prodDir,creds=self.credentials)
                print(h5files)
                for f in h5files:
                    tiff = db.atms.unpack(f)
                    db.atms.ingest(tiff,dbname=self.db)


            else:
                files = None

        else:
            raise NotImplementedError('select product is currently not implemented, please check back with later versions')

        return

    def process(self,product, date):
        date = utils.decode_date(date)

        if product in ['atms','viirs','landsat']:
            dateDir = os.path.join(self.ftchOut,date.strftime('%Y%m%d'))
            prodDir = os.path.join(dateDir,product)

            if product == 'atms':
                params = self.atmsParams
                paramKeys = list(params.keys())

                grRes = 20000
                atmsGr = rs.Grid(region=self.region.bounds.values[0],resolution=grRes)
                gr = rs.Grid(region=self.region.bounds.values[0],resolution=90)

                bathtub = downscale.Bathtub(gr,probablistic=True,elvStdDev=2.5)

                files = glob.glob(os.path.join(prodDir,'*.h5'))
                swaths = fetch.groupSwathFiles(files)
                ds = list(map(lambda x: rs.Atms.read(x[1],x[0]),swaths))

                waterFrac = map(lambda x: proc.Atms.getWaterFraction(x),ds)
                gridded = map(lambda x: rs.mapping.coregister(x,to=atmsGr),waterFrac)

                if 'hand' in paramKeys:
                    hand = rs.Arbitrary.read(params['hand'],bandNames=['hand'],time=ds[0].attrs['date'])\
                            .sel(dict(lat=slice(gr.north,gr.south),lon=slice(gr.west,gr.east)))
                else:
                    raise ValueError('hand file data source must be specified in configuration for atms processing')

                # if 'seed' in paramKeys:
                #     seed = rs.Arbitrary.read(params['seed'])
                # else:
                #     # need to add warning for not using seed file
                #     pass

                if 'daily' in paramKeys:
                    if params['daily'] in [True,'True','true',1]:
                        fWater = xr.concat(gridded,dim='time').mean(dim='time').expand_dims('time').sel(band='water')
                        fWater.attrs['resolution'] = rs.meters2dd((atmsGr.yy.mean(),atmsGr.xx.mean()),scale=grRes)
                        waterMap = bathtub.apply(fWater,hand,parallel=False)
                        waterMap.raster.writeGeotiff(prodDir,self.conf['name'],noData=0)

                    elif params['daily'] in [False,'False','false',0]:
                        gridded = map(lambda x: x.attrs['resolution'])
                        waterMap = map(lambda x: bathtub.apply(x,hand),waterFrac)
                        for i,x in enumerate(waterMap):
                            x.raster.writeGeotiff(prodDir,self.conf['name'],noData=-9999)
                    else:
                        raise ValueError('type boolean required for daily parameter in atms processing')
                else:
                    raise ValueError('daily parameter in atms processing is required')

            elif product == 'viirs':
                params = self.viirsParams
                paramKeys = list(params.keys())

                gr = rs.Grid(region=self.region.bounds.values[0],resolution=500)

                files = glob.glob(os.path.join(prodDir,'*.h5'))
                ds = map(lambda x: rs.Viirs.read(x),files)
                proj = list(map(lambda x: rs.mapping.reproject(x,outEpsg='4326',outResolution=500,method='nearest'),ds))
                gridded = map(lambda x: rs.mapping.coregister(x,to=gr),proj)

                # apply the internal mask for all rasters
                masked = map(lambda x: x.raster.applyMask(), gridded)

                mosaic = xr.concat(masked,dim='time').mean(dim='time').expand_dims('time')
                mosaic.coords['time'] =  proj[0].coords['time']

                mndwi = mosaic.raster.normalizedDifference(band1='M4',band2='I3',outBandName='mndwi')

                if 'downscale' in paramKeys:
                    print(params['downscale'])
                    if params['downscale'] in [False,'False','false',0]:
                        waterMap = proc.Viirs.getWaterMask(mndwi)
                        waterMap.attrs = proj[0].attrs
                        waterMap.raster.writeGeotiff(prodDir,self.conf['name'],noData=-9999)
                    else:
                        raise NotImplementedError()
                else:
                    raise ValueError('downscale parameter in viirs processing is required')

            elif product == 'landsat':
                params = self.landsatParams
                paramKeys = list(params.keys())

                gr = rs.Grid(region=self.region.bounds.values[0],resolution=90)

                # for subdir, dirs, files in os.walk('./'):
                #     for file in files:
                #         do some stuff
                #         print file

                files = glob.glob(os.path.join(prodDir,'*_MTL.txt'))

                for f in files:

                    l = rs.Landsat.read('/Users/loaner/rs_data/landsat/LC08_L1TP_132048_20180306_20180319_01_T1_MTL.txt')
                    lUpsamp = rs.mapping.reduceNeighborhood(l,window=3,reducer='mean',reduceResolution=True)
                    lProj = rs.mapping.reproject(lUpsamp,outEpsg='4326',outResolution=90)
                    lRemap = rs.mapping.coregister(lProj,to=gr)


            else:
                raise NotImplementedError('select product is currently not implemented, please check back with later versions')

        else:
            raise NotImplementedError('select product is currently not implemented, please check back with later versions')

        return


def main():
    fire.Fire(hydrafloods)
    return
