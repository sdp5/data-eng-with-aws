# Download datasets from AWS S3 Bucket

import configparser
import boto3

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')
TOKEN = config.get('AWS', 'TOKEN')
REGION = config.get('AWS', 'REGION')

# creating a S3 resource via boto3.resource
s3 = boto3.resource('s3',
                    region_name=REGION,
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET,
                    aws_session_token=TOKEN)

# creating a S3 client via boto3.client
s3_client = boto3.client('s3',
                         region_name=REGION,
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET,
                         aws_session_token=TOKEN)

S3_BUCKET_NAME = "udacity-dend"

# access/get the S3 bucket
bucket = s3.Bucket(S3_BUCKET_NAME)

# list all objects' names ('filenames') inside this bucket
for bucket_objects in bucket.objects.all():
    print(bucket_objects.key)

# getting all objects with prefix `log_data` inside the bucket `udacity-dend`
log_data_objects = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="log_data")

for log_data_object in log_data_objects['Contents']:
    print(log_data_object['Key'])

# download the sample files in a `data` folder
bucket.download_file('song-data/C/O/V/TRCOVDF128F42982C6.json', './data/song_data_sample.json')
bucket.download_file('log_data/2018/11/2018-11-06-events.json', './data/log_data_sample.json')

# download the third dataset
bucket.download_file('log_json_path.json', './data/log_json_path.json')
