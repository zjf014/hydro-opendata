"""Top-level package for wis-s3api."""
from .wis_s3api import paras

__author__ = """Jianfeng Zhu"""
__email__ = 'zjf014@gmail.com'
__version__ = '0.0.10'

# endpoint_url = 'http://minio.waterism.com:9000'
# access_key = 'JKhbLNL0jNKqbjn4'
# secret_key = '0RDubDRBIrC2WOHAP4nHtYP28TXtVj8H'
# bucket_path = 'test/geodata/'

import os

home_path = os.environ['HOME']

if os.path.exists(os.path.join(home_path,'.wiss3api')):
    pass
    # rows = 0
    # for line in open(os.path.join(home_path,'.wiss3api')):
    #     key = line.split('=')[0].strip()
    #     value = line.split('=')[1].strip()
    #     # print(key,value)
    #     if key == 'endpoint_url':
    #         main.endpoint_url = value
    #     elif key == 'access_key':
    #         main.access_key = value
    #     elif key == 'secret_key':
    #         main.secret_key = value
    #     elif key == 'bucket_path':
    #         main.bucket_path = value
else:
    
    access_key = input('access_key:')
    secret_key = input('secret_key:')

    f = open(os.path.join(home_path,'.wiss3api'),'w')
    f.write('endpoint_url = ' + paras['endpoint_url'])
    f.write('\naccess_key = ' + access_key)
    f.write('\nsecret_key = ' + secret_key)
    f.write('\nbucket_path = ' + paras['bucket_path'])
    f.close()
