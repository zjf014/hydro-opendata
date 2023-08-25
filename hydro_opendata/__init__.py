"""Top-level package for hydro_opendata."""
import pathlib
import os

from .common import minio_paras


__author__ = """Jianfeng Zhu"""
__email__ = 'zjf014@gmail.com'
__version__ = '0.0.1'

home_path = str(pathlib.Path.home())

if os.path.exists(os.path.join(home_path,'.wisminio')):
    pass
else:
    
    access_key = input('minio_access_key:')
    secret_key = input('minio_secret_key:')

    f = open(os.path.join(home_path,'.wisminio'),'w')
    f.write('endpoint_url = ' + minio_paras['endpoint_url'])
    f.write('\naccess_key = ' + access_key)
    f.write('\nsecret_key = ' + secret_key)
    f.write('\nbucket_name = ' + minio_paras['bucket_name'])
    f.close()