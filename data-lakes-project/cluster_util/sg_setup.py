import boto3
from .constants import REGION


def make_sg():
    """
    Makes an SG for the Spark Cluster that allows SSH access.
    :return: SecurityGroup object.
    """
    ec2 = boto3.resource("ec2", region_name=REGION)
    sg = ec2.create_security_group(
        GroupName="SparkMasterSG",
        Description="Security Group for EMR master node."
    )
    sg.authorize_ingress(
        CidrIp="0.0.0.0/0",
        FromPort=22,
        ToPort=22,
        IpProtocol="tcp"
    )
    return sg


def delete_sg(sg_id: str):
    """
    Deletes the given security group.
    :param sg_id:
    :return:
    """
    boto3.client("ec2", region_name=REGION)\
        .delete_security_group(GroupId=sg_id)
    print("INFO: SG Deleted")
