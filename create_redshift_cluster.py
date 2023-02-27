#!/usr/bin/env python
# encoding: utf-8

# import libraries
import json
import configparser
import time
import pandas as pd
import boto3

# load config parameters
config = configparser.ConfigParser()
config.read_file(open("cluster_config.cfg", encoding="utf-8"))

# access acount
KEY 			= config.get("AWS", "KEY")
SECRET			= config.get("AWS","SECRET")

# cluster config
DWH_CLUSTER_TYPE 	= config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES       = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE		= config.get("DWH", "DWH_NODE_TYPE")

# redshift database config
DWH_DB			= config.get("DB", "DWH_DB")
DWH_DB_USER		= config.get("DB", "DWH_DB_USER")
DWH_DB_PASSWORD		= config.get("DB", "DWH_DB_PASSWORD")
DWH_DB_PORT		= config.get("DB", "DWH_DB_PORT")

# iam cofig
DWH_IAM_ROLE_NAME	= config.get("IAM", "DWH_IAM_ROLE_NAME")
DWH_CLUSTER_IDENTIFIER	= config.get("IAM", "DWH_CLUSTER_IDENTIFIER")

print("Get Configuration Parameters Successfully")


# init aws servicies
try:
    ec2 = boto3.resource("ec2",
                         region_name="us-west-2",
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET)
    s3 = boto3.resource("s3",
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)
    redshift = boto3.client("redshift",
                            region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)
    iam = boto3.client("iam",
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET)
except Exception as e:
    print(e)


# create IAM role
try:
    print("1.1 Creating IAM role")
    dwhRole = iam.create_role(
        Path = "/",
        RoleName = DWH_IAM_ROLE_NAME,
        Description = "Allow RedShift to access services",
        AssumeRolePolicyDocument= json.dumps(
        {
            "Statement": [{"Action": "sts:AssumeRole",
                        "Effect":"Allow",
                        "Principal":{"Service":"redshift.amazonaws.com"}}],
            "Version": "2012-10-17"
        })
    )
except Exception as e:
    print(e)


# create IAM role
print("1.2 Attach IAM policy")
attach_policy = iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                                )

print(attach_policy["ResponseMetadata"]["HTTPStatusCode"])
print("Policy Attached")

# get IAM role arn
print("1.3 Get the IAM role ARN")
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)["Role"]["Arn"]

print("Role Arn: ", roleArn)


# create the redshift cluster
print("1.4 Create the Redshift Cluster")
try:
    response = redshift.create_cluster(        
        # add parameters for hardware
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        # add parameters for identifiers & credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,

        # add parameter for role (to allow s3 access)
        IamRoles=[roleArn] 
    )
    
except Exception as e:
    print(e)


# check and print cluster properities & status
print("1.5 Check Cluster Properities")

def prettyRedshiftProps(props):
    
    pd.set_option('display.max_colwidth', None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus",
                  "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    data = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=data, columns=["Key", "Value"])

# Check Cluster properities
myClusterProps = redshift.describe_clusters(
    ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

properities_df = prettyRedshiftProps(myClusterProps)


while properities_df.loc[2, "Value"] != "available":
    # wait 5 second before checking every status
    time.sleep(5)

    # check properities again
    myClusterProps = redshift.describe_clusters(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    
    properities_df = prettyRedshiftProps(myClusterProps)

    
    print("Still Creating the cluster")

print("Cluster Created Successfully")


# 2.2 Take note of the cluster endpoint and role ARN
DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)


# open TCB port to access redshift cluster endpoint
try:
    # print deafault security group
    vpc = ec2.Vpc(id=myClusterProps["VpcId"])
    deafaultSg = list(vpc.security_groups.all())[0]
    print("Default Security Group:", deafaultSg)

    # edit security group features
    deafaultSg.authorize_ingress(
        GroupName=deafaultSg.group_name,
        CidrIp="0.0.0.0/0",
        IpProtocol="TCP",
        FromPort=int(DWH_DB_PORT),
        ToPort=int(DWH_DB_PORT)
    )

except Exception as e:
    print(e)


#delete the cluster (uncomment to delete the cluster and run the script again)
redshift.delete_cluster(
    ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
print("1.6 Cluster Deleted Successfully")