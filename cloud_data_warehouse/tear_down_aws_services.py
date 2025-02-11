# Teardown AWS Services

import configparser

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

# AWS
KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')
TOKEN = config.get('AWS', 'TOKEN')
REGION = config.get('AWS', 'REGION')

# DWH
DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')

# IAM
IAM_ROLE_NAME = config.get('IAM', 'ROLE_NAME')
IAM_ROLE_ARN = config.get('IAM', 'ROLE_ARN')

# Create a Redshift client and delete cluster
import boto3

redshift_client = boto3.client('redshift',
                                region_name=REGION,
                                aws_access_key_id=KEY,
                                aws_secret_access_key=SECRET,
                                aws_session_token=TOKEN)

redshift_client.delete_cluster(
    ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True
)

# Detach IAM role and delete
iam = boto3.client('iam',
                   region_name=REGION,
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET,
                   aws_session_token=TOKEN)

# detach the assigned policy from the created ROLE
iam.detach_role_policy(RoleName=IAM_ROLE_NAME, PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess')
iam.detach_role_policy(RoleName=IAM_ROLE_NAME, PolicyArn='arn:aws:iam::aws:policy/AmazonRedshiftQueryEditor')

# delete the role
iam.delete_role(RoleName=IAM_ROLE_NAME)
