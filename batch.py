from __future__ import print_function

from PIL import Image
import os
import json
import urllib
import boto3

client = boto3.client('s3')

bucket = "aft-vbi-pds"
prefix='bin-images'

def lambda_handler():
    response = client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        MaxKeys=1,

    )
    for image in response['Contents']:
        if "jpg" in image['Key']:
            print(image['Key'])

            new_file=image['Key'].split('/',2)[1]
            print (new_file)
            client.download_file(bucket, image['Key'], new_file)

            image = Image.open(new_file)
            image = image.convert('L')
            (name, extension) = os.path.splitext(new_file)
            image.save(name + '_changed' + extension)
            os.remove(new_file)

    asd = client.get_object(
        Bucket=bucket,
        Key=(image['Key']),
)
    print(asd)


lambda_handler()