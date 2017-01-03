from __future__ import print_function

from PIL import Image
import os

import boto3

client = boto3.client('s3')

bucket = "aft-vbi-pds"
prefix='bin-images'

def batch():
    response = client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        MaxKeys=2,
    )
    for image in response['Contents']:
        if "jpg" in image['Key']:
            print(image['Key'])

            new_file=image['Key'].split('/',2)[1]
            client.download_file(bucket, image['Key'], new_file)

            image = Image.open(new_file)
            image = image.convert('L')
            (name, extension) = os.path.splitext(new_file)
            filename = (name + '_changed' + extension)
            image.save(filename)
            os.remove(new_file)
            client_new=boto3.resource('s3')
            print(client_new.meta.client.upload_file(filename, 'awsome-batch', filename))

batch()