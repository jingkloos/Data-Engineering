import boto3
import configparser
from botocore.exceptions import *
import json
import logging
import logging.config
from pathlib import Path
import argparse
import time
#To create redshift cluster, you need ec2 security group, IAM role
#redshift cluster runs on a EC2 VPC instance and need IAM role to access other aws services

#set up logger with logging.conf file
logging.config.fileConfig('logging.conf')
logger=logging.getLogger()

#read cluster configs from file cluster.conf
config=configparser.ConfigParser()
with open('cluster.conf','r') as f:
    config.read_file(f)

def create_IAM_role(iam_client):
    '''
    create IAM (identity and access management) role based on cluster.conf
    :param -> iam_client: IAM service client instance
    :return -> True if IAM role created and policy applied successfully
    '''
    role_name=config.get('IAM_ROLE','NAME')
    role_desc=config.get('IAM_ROLE','DESCRIPTION')
    role_policy_arn=config.get('IAM_ROLE','POLICY_ARN')
    logger.info('Creating IAM role with name: {},description: {} and policy: {}'.format(role_name,role_desc,role_policy_arn))
    with open('role_policy_doc.json') as f:
        role_policy_doc=json.dumps(json.load(f))

    role_exist=True
    try:
        iam_client.get_role(RoleName=role_name)
        logger.info('Role {} already exits'.format(role_name))
        return True
    except Exception as e:
        logger.info('Role {} does not exist, start creating it'.format(role_name))
        
    #create role
    try:
        create_response=iam_client.create_role(
            Path='/',
            RoleName=role_name,
            Description=role_desc,
            AssumeRolePolicyDocument=role_policy_doc
        )
        logger.debug('Returned response from IAM client for creating role: {}'.format(create_response))
        logger.info('Role create response code:{}'.format(create_response['ResponseMetadata']['HTTPStatusCode']))
    except Exception as e:
        logger.error('Error occured while creating role:{}'.format(e))
        return False
    
    #attach policy
    try:
        policy_response=iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn=role_policy_arn
        )
        logger.debug('Returned response from IAM client for applying policy to the role: {}'.format(policy_response))
        logger.info('Attach policy response code: {}'.format(policy_response['ResponseMetadata']['HTTPStatusCode']))
    except Exception as e:
        logger.error('Error occured while applying policy:{}'.format(e))
        return False
    return create_response['ResponseMetadata']['HTTPStatusCode'] == 200 and policy_response['ResponseMetadata']['HTTPStatusCode']==200

def delete_IAM_role(iam_client):
    '''
    delete the IAM role from cluster.conf file
    :param -> iam_client IAM client instance
    :return -> True if role deleted successfully or role doesn't exist
    '''
    role_name=config.get('IAM_ROLE','NAME') 
    role_policy_arn=config.get('IAM_ROLE','POLICY_ARN')
    existing_roles=[role['RoleName'] for role in iam_client.list_roles()['Roles']]
    if role_name not in existing_roles:
        logger.info('Role {} does not exist.'.format(role_name))
        return True
    
    logger.info('Start deleting role {}'.format(role_name))
    #detach policy first and then delete role
    try:
        detach_response=iam_client.detach_role_policy(RoleName=role_name,PolicyArn=role_policy_arn)
        logger.debug('Response for policy detach from IAM role: {}'.format(detach_response))
        logger.info('Response code for policy detach from IAM role: {}'.format(detach_response['ResponseMetadata']['HTTPStatusCode']))
        delete_response=iam_client.delete_role(RoleName=role_name)
        logger.debug('Response for deleting IAM role: {}'.format(delete_response))
        logger.info('Response code for deleting IAM role: {}'.format(delete_response['ResponseMetadata']['HTTPStatusCode']))
    except Exception as e:
        logger.error('Exception occured while deleting role: {}'.format(e))
        return False
    return True if detach_response['ResponseMetadata']['HTTPStatusCode']==200 and delete_response['ResponseMetadata']['HTTPStatusCode']==200 else False

def create_ec2_security_group(ec2_client):
    '''
    create an ec2 security group with group name in cluster.conf file
    :param -> ec2_client: an ec2 client instance
    :return -> True if security group exists or created successfully
    '''
    
    group_name=config.get('SECURITY_GROUP','NAME')
    group_desc=config.get('SECURITY_GROUP','DESCRIPTION')
    logger.info('Creating security group {}'.format(group_name))

    if get_ec2_security_group(ec2_client,group_name):
        logger.info('Group {} already exists!'.format(group_name))
        return True
    # get VPC ID
    vpc_id=ec2_client.describe_security_groups()['SecurityGroups'][0]['VpcId']
    try:
        response=ec2_client.create_security_group(
            Description=group_desc,
            GroupName=group_name,
            VpcId=vpc_id,
            DryRun=False # Checks whether you have the required permissions for the action, without actually making the request, and provides an error response
        )
        logger.debug('Returned response from creating ec2 security group: {}'.format(response))
        logger.info('Response code from creating ec2 security group: {}'.format(response['ResponseMetadata']['HTTPStatusCode']))

        logger.info('Authorizing security group ingress')
        ec2_client.authorize_security_group_ingress(
            GroupId=response['GroupId'],
            GroupName=group_name,
            FromPort=int(config.get('INBOUND_RULE','PORT_RANGE')),
            ToPort=int(config.get('INBOUND_RULE','PORT_RANGE')),
            CidrIp=config.get('INBOUND_RULE','CIDRIP'),
            IpProtocol=config.get('INBOUND_RULE','PROTOCOL'),
            DryRun=False
        )
    except Exception as e:
        logger.error('Error occured while from creating ec2 security group:{}'.format(e))
        return False
    return response['ResponseMetadata']['HTTPStatusCode'] == 200 


def delete_ec2_security_group(ec2_client):
    '''
    delete the ec2 security group from cluster.conf file
    :param -> ec2_client: an ec2 client instance
    :return -> True if security group was deleted successfully
    '''
    group_name=config.get('SECURITY_GROUP','NAME')
    logger.info('Deleting security group {}'.format(group_name))
    group=get_ec2_security_group(ec2_client,group_name)
    if not group:
        logger.info('Group {} does not exists!'.format(group_name))
        return True
    try:
        response=ec2_client.delete_security_group(
            GroupId=group['GroupId'],
            GroupName=group_name,
            DryRun=False # Checks whether you have the required permissions for the action, without actually making the request, and provides an error response
        )
        logger.debug('Returned response from deleting ec2 security group: {}'.format(response))
        logger.info('Response code from deleting ec2 security group: {}'.format(response['ResponseMetadata']['HTTPStatusCode']))
    except Exception as e:
        logger.error('Error occured while from deleting ec2 security group:{}'.format(e))
        return False
    return response['ResponseMetadata']['HTTPStatusCode'] == 200 
    
def get_ec2_security_group(ec2_client,group_name):
    '''
    :param -> ec2_client: an ec2 client instance
    :param -> group_name: string 
    return the security group {group_name}, None if the group doesn't exist 
    '''
    groups=ec2_client.describe_security_groups(Filters=[{'Name':'group-name','Values':[group_name]}])['SecurityGroups']
    if len(groups)>0:
        return groups[0]
    

def create_cluster(redshift_client,iam_role_arn,vpc_security_group_id):
    '''
    create a redshift cluster using created iam role and security group
    :param -> redshift_client: redshift client instance
    :param -> iam_role_arn: IAM role arn to give permission to cluster to communicate with other AWS services
    :param -> vpc_security_group_id: vpc group for network setting for cluster
    :return -> True if cluster created successfully
    '''  
    #get cluster configs
    # Cluster Hardware config
    cluster_type = config.get('DW','DW_CLUSTER_TYPE')
    node_type =  config.get('DW', 'DW_NODE_TYPE')
    num_nodes = int(config.get('DW', 'DW_NUM_NODES'))

    # Cluster identifiers and credentials
    cluster_identifier = config.get('DW','DW_CLUSTER_IDENTIFIER')
    db_name = config.get('DW', 'DW_DB')
    database_port=int(config.get('DW','DW_PORT'))
    master_username = config.get('DW', 'DW_DB_USER')
    master_user_password = config.get('DW', 'DW_DB_PASSWORD')
    try:
        redshift_client.describe_clusters(ClusterIdentifier=cluster_identifier)
        logger.info('Cluster {} already exist!'.format(cluster_identifier))
        return True
    except Exception as e:
        logger.info('Cluster {} does not exist, starting creating it'.format(cluster_identifier))
    #create cluster
    try:
        response=redshift_client.create_cluster(
            DBName=db_name,
            ClusterIdentifier=cluster_identifier,
            ClusterType=cluster_type,
            NodeType=node_type,
            NumberOfNodes=num_nodes,
            MasterUsername=master_username,
            MasterUserPassword=master_user_password,
            VpcSecurityGroupIds=vpc_security_group_id,
            IamRoles = [iam_role_arn]
        )
        logger.debug('Returned response from creating redshift cluster: {}'.format(response))
        logger.info('Create cluster response code: {}'.format(response['ResponseMetadata']['HTTPStatusCode']))
    except Exception as e:
        logger.error('Error occured while creating cluster:{}'.format(e))
        return False
    return response['ResponseMetadata']['HTTPStatusCode'] == 200

def delete_cluster(redshift_client):
    '''
    delete the cluster with identifier from cluster.conf file
    :param -> redshift_client: redshift client instance
    :return -> True if cluster is deleted successfully
    '''
    cluster_identifier=config.get('DW','DW_CLUSTER_IDENTIFIER')
    logger.info('Start deleting cluster {}'.format(cluster_identifier))
    try:
        cluster_status=get_cluster_status(redshift_client,cluster_identifier)
        while not cluster_status:
            logger.info('Cannot delete cluster. Waiting for it to become available')
            time.sleep(10)
            cluster_status=get_cluster_status(redshift_client,cluster_identifier)
        response=redshift_client.delete_cluster(
                ClusterIdentifier=cluster_identifier,
                SkipFinalClusterSnapshot=True
            )
        logger.debug('Response from deleting cluster: {}'.format(response))
        logger.info('Response code from deleting cluster: {}'.format(response['ResponseMetadata']['HTTPStatusCode']))
    except Exception as e:
        logger.error('Error occured while deleting cluster: {}'.format(e))
        return False
    return response['ResponseMetadata']['HTTPStatusCode']==200


def get_cluster_status(redshift_client,cluster_identifier):
    '''
    return cluster status of cluster {cluster_identifier}
    :param -> redshift_client: redshift client instance
    :param -> cluster_identifier: string
    :return -> True if cluster status is in available list
    '''
    try:
        response=redshift_client.describe_clusters(ClusterIdentifier=cluster_identifier)
        cluster_status=response['Clusters'][0]['ClusterStatus']
        logger.info('Cluster status:  {}'.format(cluster_status))
        return cluster_status.upper() in ('AVAILABLE','ACTIVE', 'INCOMPATIBLE_NETWORK', 'INCOMPATIBLE_HSM', 'INCOMPATIBLE_RESTORE', 'INSUFFICIENT_CAPACITY', 'HARDWARE_FAILURE')
    except Exception as e:
        logger.error('Error occured while getting cluster status: {}'.format(e))
        return False

def boolean_parser(input):
    if input.upper() not in ('TRUE','FALSE'):
        logger.error('Invalid argument {}, it must be TRUE or FALSE'.format(input))
        raise ValueError('Not a valid boolean string')
    return input.upper()=='TRUE'

if __name__=='__main__':
    #parse arguments
    parser=argparse.ArgumentParser(description='An utility to spin up a redshift cluster.\
                It creates IAM role and ec2 security group,set up ingress parameters and finally spin up a redshift cluster.')
    required=parser.add_argument_group('required arguments')
    optional=parser.add_argument_group('optional arguments')
    required.add_argument('-c','--create',type=boolean_parser,metavar='',required=True,
                            help='True or False. Create IAM roles, security group and redshift cluster')
    optional.add_argument('-d','--delete',type=boolean_parser,metavar='',required=False,default=False,
                            help='True or False. Delete IAM roles, security group and redshift cluster.\
                                WARNING: Deletes the Redshift cluster, IAM role and security group.')
    optional.add_argument('-v','--verbose',type=boolean_parser,metavar='',required=False,default=True,
                            help='Decrease ouput verbosity. Default set to DEBUG')
    args=parser.parse_args()
    logger.info('ARGS:{}'.format(args))

    if not args.verbose:
        logger.setLevel(logging.INFO)
        logger.info('Logging level set to INFO.')
    
    iam=boto3.client(service_name='iam',region_name='us-west-1', aws_access_key_id=config.get('AWS', 'Key'), aws_secret_access_key=config.get('AWS', 'SECRET'))
    ec2=boto3.client(service_name='ec2',region_name='us-west-1', aws_access_key_id=config.get('AWS', 'Key'), aws_secret_access_key=config.get('AWS', 'SECRET'))
    redshift=boto3.client(service_name='redshift',region_name='us-west-1', aws_access_key_id=config.get('AWS', 'Key'), aws_secret_access_key=config.get('AWS', 'SECRET'))

    logger.info('Clients setup for all required services')

    if args.create:
        if create_IAM_role(iam):
            logger.info('IAM role created successfully! Creating security group......')
            if create_ec2_security_group(ec2):
                logger.info('Security group created successfully! Creating redshift cluster......')
                role_arn=iam.get_role(RoleName=config.get('IAM_ROLE', 'NAME'))['Role']['Arn']
                vpc_security_group_id = get_ec2_security_group(ec2,config.get('SECURITY_GROUP', 'NAME'))['GroupId']
                create_cluster(redshift,role_arn,[vpc_security_group_id])
            else:
                logger.error('Failed to create security group')
        else:
            logger.error('Failed to create IAM role')

    elif args.delete:
        delete_cluster(redshift)
        while not delete_ec2_security_group(ec2):
            time.sleep(100)
        delete_IAM_role(iam)        

