import os
import glob
import datetime
import logging
from landsat.google_download import GoogleDownload
from landsat.update_landsat_metadata import update_metadata_lists

from . import dbio

stname = 'obs.landsat'

def ingest(tiffs,dbname=None):
    if dbname:
        # dt = date.strftime('%Y-%m-%d')

        mname = os.path.splitext(tiffs[0])[0].split('_')[:-1]
        mname.append('MTL.txt')
        metadata = _parseMetadata('_'.join(mname))

        date = '{0} {1}{2}'.format(metadata['DATE_ACQUIRED'],metadata['SCENE_CENTER_TIME'][:-3], ' UTC')
        dt = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S.%f %Z')

        for tiff in tiffs:
            fname, ext = os.path.splitext(tiff)
            band = fname.split('_')[-1]
            if band == 'BQA':
                nd = 1
            else:
                nd = 0
            if band != 'B8':
                inpath = os.path.abspath(tiff)
                dbio.ingest(dbname, inpath, dt, band, stname,nodata=nd)
    else:
        raise ValueError('dbname must be specified')

    return

def fetch(date,p,r,outdir='./',updateScenes=False,maxClouds=100):
    if outdir[-1] != '/':
        outdir = outdir+'/'

    sDate = (date - datetime.timedelta(1)).strftime('%Y-%m-%d')
    eDate = (date + datetime.timedelta(1)).strftime('%Y-%m-%d')

    downloader = GoogleDownload(sDate,eDate,8,path=p, row=r,
                                max_cloud_percent=maxClouds,
                                output_path=outdir)

    search = list(downloader.scenes_low_cloud.SCENE_ID)

    if len(search) == 1:
        scene = search[0]
        downloader.download()

        print(os.path.join(outdir,scene,'*.TIF'))

        tiffs = glob.glob(os.path.join(outdir,scene,'*.TIF'))
        print(tiffs)

    else: tiffs = None

    return tiffs

def _parseMetadata(metadata):
    with open(metadata,'r') as f:
        data = f.read()

    split_metadata = data.split('\n')

    output = {}
    for x in split_metadata:
        if "=" in x:
            line = x.split("=")
            output[line[0].strip()] = line[1].strip()
            clean_output = {key: item.strip('"') for key, item in output.items()}

    return clean_output
