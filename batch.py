from __future__ import print_function

from PIL import Image
import os
from PIL.ExifTags import TAGS
import PIL.ExifTags

import boto3
from boto3.dynamodb.conditions import Key

from boto3 import Session

client = boto3.client('s3')

bucket = "awsome-batch"
prefix = ''

session = Session(region_name='us-east-1')
dynamodb = session.resource('dynamodb')
table = dynamodb.Table('ExifTable')

flash_status = {'0': 'No Flash',
                '1': 'Fired',
                '5': 'Fired, Return not detected',
                '7': 'Fired, Return detected',
                '8': 'On, Did not fire',
                '9': 'On, Fired',
                'd': 'On, Return not detected',
                'f': 'On, Return detected',
                '10': 'Off, Did not fire',
                '14': 'Off, Did not fire, Return not detected',
                '18': 'Auto, Did not fire',
                '19': 'Auto, Fired',
                '1d': 'Auto, Fired, Return not detected',
                '1f': 'Auto, Fired, Return detected',
                '20': 'No flash function',
                '30': 'Off, No flash function',
                '41': 'Fired, Red-eye reduction',
                '45': 'Fired, Red-eye reduction, Return not detected',
                '47': 'Fired, Red-eye reduction, Return detected',
                '49': 'On, Red-eye reduction',
                '4d': 'On, Red-eye reduction, Return not detected',
                '4f': 'On, Red-eye reduction, Return detected',
                '50': 'Off, Red-eye reduction',
                '58': 'Auto, Did not fire, Red-eye reduction',
                '59': 'Auto, Fired, Red-eye reduction',
                '5d': 'Auto, Fired, Red-eye reduction, Return not detected',
                '5f': 'Auto, Fired, Red-eye reduction, Return detected'}

get_float = lambda x: float(x[0]) / float(x[1])

####### https://gist.github.com/maxbellec/dbb60d136565e3c4b805931f5aad2c6d #####
def convert_to_degrees(value):
    d = get_float(value[0])
    m = get_float(value[1])
    s = get_float(value[2])
    return d + (m / 60.0) + (s / 3600.0)


def get_lat_lon(info):
    try:
        gps_latitude = info[34853][2]
        gps_latitude_ref = info[34853][1]
        gps_longitude = info[34853][4]
        gps_longitude_ref = info[34853][3]
        lat = convert_to_degrees(gps_latitude)
        if gps_latitude_ref != "N":
            lat *= -1

        lon = convert_to_degrees(gps_longitude)
        if gps_longitude_ref != "E":
            lon *= -1
        return lat, lon
    except KeyError:
        return None
##############################################################################

def batch():
    response = client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        MaxKeys=1000,
    )

    for image in response['Contents']:
        exif_data = {}
        if image['Key'].endswith(".jpg") or image['Key'].endswith(".JPG"):
            new_file = image['Key']
            client.download_file(bucket, image['Key'], new_file)
            image = Image.open(new_file)
            info = image._getexif()
            os.remove(new_file)
            for k, v in info.items():
                if k in TAGS:
                    exif_data[PIL.ExifTags.TAGS[k]] = v

            try:
                if 'Flash' in exif_data:
                    flash_code = (str(exif_data['Flash']))
                    for key, val in flash_status.items():
                        if flash_code == key:
                            flash_value = val
                else:
                    flash_value = "N/A"
                request = table.query(
                    TableName='ExifTable',
                    Select="ALL_ATTRIBUTES",
                    ScanIndexForward=False,
                    KeyConditionExpression=Key('ImgId').eq(str(new_file))
                )
                if request['Count'] != 1:
                    print("writing to db...")
                    table.put_item(
                        Item={
                            'ImgId': str(new_file),
                            'Software': exif_data['Software'],
                            'Flash': flash_value,
                            'Brand': exif_data['Make'],
                            'Model': exif_data['Model'],
                            'Date': exif_data['DateTimeOriginal'],
                            'GpsInfo': str(get_lat_lon(info))
                        }
                    )
                else:
                    print("already in db...")
            except Exception as e:
                print("error is: ", e)


if __name__ == '__main__':
    batch()