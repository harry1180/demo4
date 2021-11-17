import sys
import boto3
import gzip
import json
import csv

from os.path import isfile
from io import BytesIO
from datetime import datetime


BUCKET_NAME = "vz-bhrx-log"
PREFIX = 'bhrx-tr069-snapshot/parameter=snapshot_1/'
PARAMS = [
    'Device.Services.X_VZ-COM_DeviceConfig.ParentalControl.RuleNumberofEntries',
    'Device.Services.X_VZ-COM_DeviceConfig.ParentalControl.Enabled'
]
# Report.
global result
result = {}
HEADERS = ['datetime'] + PARAMS
REPORT_FILE = "report.csv"


def _process(json_data):
    """Processing raw json data and creating summary result.
    """
    global result
    for param in PARAMS:
        if param not in result:
            result[param] = {'found': 0, 'enabled': 0}
        if param in json_data.keys():
            result[param]['found'] += 1
            result[param]['enabled'] += 1 if result[param] else 0


def read_from_s3(limit=0, current=True):
    prefix = PREFIX
    if current:
        # Current will consider only today's router details.
        prefix += "date={}/".format(datetime.strftime(datetime.today(), '%Y%m%d'))

    print("Reading data from S3 bucket={} prefix={}, please wait ...".format(BUCKET_NAME, prefix))
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(BUCKET_NAME)
    counter = 0

    for obj in bucket.objects.filter(Prefix=prefix).all():
        if obj.key == 'bhrx-tr069-snapshot/':
            continue

        counter += 1
        print("{} ... ".format(obj.key), end='')
        body = obj.get()['Body'].read()

        with gzip.GzipFile(fileobj=BytesIO(body), mode='rb') as _fh:
            try:
                _process(json.load(_fh))
            except Exception:
                print(" Error")
                continue

        print(" done")
        if limit and counter == limit:
            break


def save():
    """Save final result into report CSV file.
    """
    global result
    init = 0

    if result is None:
        res = ['0:0'] * len(PARAMS)
    else:
        res = ['{}:{}'.format(result[key]['found'], result[key]['enabled']) for key in result]

    # Adding datetime.
    res.insert(0, datetime.strftime(datetime.today(), '%Y%m%d%H'))
    res = [dict(zip(HEADERS, res))]

    print(res)

    if not isfile(REPORT_FILE):
        init = 1

    with open(REPORT_FILE, 'a', encoding='UTF-8', newline='') as _fh:
        writer = csv.DictWriter(_fh, fieldnames=HEADERS)
        if init:
            writer.writeheader()
        writer.writerows(res)


if __name__ == '__main__':
    help_txt = "python analyser <current>"
    try:
        current = sys.argv[1]
        try:
            current = int(current)
        except ValueError:
            sys.exit("Invalid inputs.\n" + help_txt)
    except IndexError:
        pass
    current = True if current else False

    start_time = datetime.strftime(datetime.today(), '%Y/%m/%d %H:%M:%S')
    read_from_s3(current=current)
    end_time = datetime.strftime(datetime.today(), '%Y/%m/%d %H:%M:%S')
    print("Start={} End={}".format(start_time, end_time))

    save()
