"""Main module."""
import pathlib
import os
import s3fs

minio_paras = {
    'endpoint_url' : 'http://minio.waterism.com:9000',
    'access_key' : '',
    'secret_key' : '',
    'bucket_name' : 'test'
}

home_path = str(pathlib.Path.home())

if os.path.exists(os.path.join(home_path,'.wisminio')):
    for line in open(os.path.join(home_path,'.wisminio')):
        key = line.split('=')[0].strip()
        value = line.split('=')[1].strip()
        # print(key,value)
        if key == 'endpoint_url':
            minio_paras['endpoint_url'] = value
        elif key == 'access_key':
            minio_paras['access_key'] = value
        elif key == 'secret_key':
            minio_paras['secret_key'] = value
        elif key == 'bucket_path':
            minio_paras['bucket_name'] = value
            
fs = s3fs.S3FileSystem(
    client_kwargs={"endpoint_url": minio_paras['endpoint_url']}, 
    key=minio_paras['access_key'], 
    secret=minio_paras['secret_key'] 
)


ro = {
    'client_kwargs': {'endpoint_url': minio_paras['endpoint_url']}, 
    'key': minio_paras['access_key'], 
    'secret': minio_paras['secret_key']
}