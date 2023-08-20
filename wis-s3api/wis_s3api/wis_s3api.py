"""Main module."""
import pathlib
import os

paras = {
    'endpoint_url' : 'http://minio.waterism.com:9000',
    # 'access_key' : 'JKhbLNL0jNKqbjn4',
    # 'secret_key' : '0RDubDRBIrC2WOHAP4nHtYP28TXtVj8H',
    'access_key' : '',
    'secret_key' : '',
    'bucket_path' : 'test/geodata/'
}

home_path = str(pathlib.Path.home())

if os.path.exists(os.path.join(home_path,'.wiss3api')):
    for line in open(os.path.join(home_path,'.wiss3api')):
        key = line.split('=')[0].strip()
        value = line.split('=')[1].strip()
        # print(key,value)
        if key == 'endpoint_url':
            paras['endpoint_url'] = value
        elif key == 'access_key':
            paras['access_key'] = value
        elif key == 'secret_key':
            paras['secret_key'] = value
        elif key == 'bucket_path':
            paras['bucket_path'] = value