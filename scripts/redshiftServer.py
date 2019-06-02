import sys
import pandas as pd
import json
import configparser
import boto3
import time
import psycopg2

config = configparser.ConfigParser()
config.read_file(open('../etc/conf/dwh.cfg'))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH", "DWH_PORT")
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

ec2 = boto3.resource('ec2',
                     region_name="us-west-2",
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET
                     )

s3 = boto3.resource('s3',
                    region_name="us-west-2",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                    )

iam = boto3.client('iam', aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET,
                   region_name='us-west-2'
                   )

redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )


# create role
def get_role(iam):
    # create role
    try:
        roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
        return roleArn
    except Exception as e:
        print("role already exists")

    iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description="Allow Redshift clusters to call AWS service on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {
                'Statement':
                    [{
                        'Action': 'sts:AssumeRole',
                        'Effect': 'Allow',
                        'Principal': {
                            'Service': 'redshift.amazonaws.com'
                        }
                    }],
                'Version': '2012-10-17'
            })
    )

    # attach policy
    iam.attach_role_policy(
        RoleName=DWH_IAM_ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")['ResponseMetadata']['HTTPStatusCode']

    # 1.3 Get the Iam Role
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    return roleArn


# create redshift cluster
def get_redshift_cluster(redshift, roleArn):

    try:
        redshift.create_cluster(
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            IamRoles=[roleArn]
        )
    except Exception as e:
        print(e)

    while True:
        exist_clusters = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

        if DWH_CLUSTER_IDENTIFIER == exist_clusters['ClusterIdentifier'] \
                and exist_clusters['ClusterStatus'] == 'available':
            return exist_clusters

        print('ClusterStatus: >>> ', exist_clusters['ClusterStatus'])
        time.sleep(60)


# open a incoming TCP port to access the cluster endpoint
def open_port(ec2, clusterProps):
    vpc = ec2.Vpc(id=clusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]

    try:
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        pass


def create_cluster():
    roleArn = get_role(iam)
    clusterProps = get_redshift_cluster(redshift, roleArn)
    open_port(ec2, clusterProps)

    DWH_ENDPOINT = clusterProps['Endpoint']['Address']
    # DWH_ROLE_NAME = clusterProps['IamRoles'][0]['IamRoleArn']
    conn_string = "postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_DB)
    print('Redshift connection:')
    print(conn_string)


def delete_cluster():
    try:
        redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)
    except Exception as e:
        print(e)

    try:
        while True:
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            status = myClusterProps['ClusterStatus']
            print("ClusterStatus: >>> ", status)

            if status != "deleting":
                return
            time.sleep(60)

    except Exception as e:
        pass

    finally:
        print("ClusterStatus: >>> ", 'deleted')


if __name__ == '__main__':
    option = sys.argv[1]

    if option.lower() == "--create":
        create_cluster()
    elif option.lower() == "--delete":
        delete_cluster()
    else:
        print("wrong configuration!")
