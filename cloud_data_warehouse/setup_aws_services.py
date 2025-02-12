# Provision AWS Services

import pandas as pd
import configparser
import boto3
import json
import redshift_connector

from typing import Any

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

# AWS
KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')
TOKEN = config.get('AWS', 'TOKEN')
REGION = config.get('AWS', 'REGION')

# IAM
IAM_ROLE_NAME = config.get('IAM', 'ROLE_NAME')

# DWS
DWH_CLUSTER_TYPE = config.get('DWH', 'DWH_CLUSTER_TYPE')
DWH_NUM_NODES = config.get('DWH', 'DWH_NUM_NODES')
DWH_NODE_TYPE = config.get('DWH', 'DWH_NODE_TYPE')

DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
DWH_DB = config.get('DWH', 'DWH_DB')
DWH_DB_USER = config.get('DWH', 'DWH_DB_USER')
DWH_DB_PASSWORD = config.get('DWH', 'DWH_DB_PASSWORD')
DWH_PORT = config.get('DWH', 'DWH_PORT')
DWH_ENDPOINT = config.get('DWH', 'DWH_ENDPOINT')

aws_client_iam = boto3.client('iam',
                              region_name=REGION,
                              aws_access_key_id=KEY,
                              aws_secret_access_key=SECRET,
                              aws_session_token=TOKEN)

aws_client_redshift = boto3.client('redshift',
                                    region_name=REGION,
                                    aws_access_key_id=KEY,
                                    aws_secret_access_key=SECRET,
                                    aws_session_token=TOKEN)

aws_resource_ec2 = boto3.resource('ec2',
                                  region_name=REGION,
                                  aws_access_key_id=KEY,
                                  aws_secret_access_key=SECRET,
                                  aws_session_token=TOKEN)

def create_iam_role() -> Any:
    """Create IAM role."""
    try:
        print("\nCreating a new IAM Role")

        assume_role_policy_document = json.dumps({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "redshift.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        })

        dwh_role = aws_client_iam.create_role(
            Path='/',
            # name given in my configuration file
            RoleName=IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services.",
            AssumeRolePolicyDocument=assume_role_policy_document
        )
        print(f"\nDWH Role: {dwh_role}")
    except Exception as e:
        print(e)

    # Assign policies to IAM role.
    print("\nAttaching Policy: AmazonS3ReadOnlyAccess")
    aws_client_iam.attach_role_policy(
        RoleName=IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )['ResponseMetadata']['HTTPStatusCode']

    role_arn = aws_client_iam.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']

    print(f"\nROLE ARN: {role_arn}")
    return role_arn


def create_redshift_cluster(iam_role_arn: Any) -> Any:
    """Creates RedShift Cluster."""

    try:
        response = aws_client_redshift.create_cluster(
            # Data Warehouse specs
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            PubliclyAccessible=True,    # This can be done on AWS Console as well.

            # first database of the cluster
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            # user's role to manage this Redshift (for S3)
            IamRoles=[iam_role_arn]
        )
        return response
    except Exception as e:
        print(e)


# print only a few properties as a pandas dataframe
def pretty_redshift_props(props):
    prop_keys_to_show = [
        "ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername",
        "DBName", "Endpoint", "NumberOfNodes", 'VpcId'
    ]

    x = [(k, v) for k, v in props.items() if k in prop_keys_to_show]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def get_redshift_cluster_ready() -> Any:
    """Get RedShift Cluster ready."""

    role_arn = create_iam_role()
    create_redshift_cluster(role_arn)

    # describe cluster properties with identifier DWH_CLUSTER_IDENTIFIER
    # from our redshift python client
    cluster_props = aws_client_redshift.describe_clusters(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER
    )['Clusters'][0]

    print(cluster_props)
    pretty_redshift_props(cluster_props)
    return cluster_props


def access_cluster_endpoint(redshift_cluster_props: Any):
    """Access RedShift Cluster endpoint"""

    try:
        vpc = aws_resource_ec2.Vpc(id=redshift_cluster_props['VpcId'])

        # Security Group
        default_sg = list(vpc.security_groups.all())[0]
        print(default_sg)

        default_sg.authorize_ingress(
            GroupName=default_sg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            # information insider the file `dwh.cfg`
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)


def check_cluster_connection():
    """Check redshift cluster connection"""

    conn = redshift_connector.connect(
        host=DWH_ENDPOINT,
        database=DWH_DB,
        port=int(DWH_PORT),
        user=DWH_DB_USER,
        password=DWH_DB_PASSWORD
    )
    return conn


if __name__ == "__main__":
    cluster_props = get_redshift_cluster_ready()
    # Once cluster is ready
    DWH_ENDPOINT = cluster_props['Endpoint']['Address']
    DWH_ROLE_ARN = cluster_props['IamRoles'][0]['IamRoleArn']

    # Copy these values to dwh.cfg
    print(f"DWH_ENDPOINT: {DWH_ENDPOINT}")
    print(f"DWH_ROLE_ARN: {DWH_ROLE_ARN}")

    access_cluster_endpoint(cluster_props)
    conn = check_cluster_connection()
    assert conn
