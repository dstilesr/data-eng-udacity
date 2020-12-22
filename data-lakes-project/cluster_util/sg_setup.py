import re
import boto3
import requests
from typing import Optional
from .constants import REGION


def get_public_ip() -> Optional[str]:
    """
    Get the current public IP from http://ident.me
    :return:
    """
    out = None
    try:
        rsp = requests.get("https://ident.me")
        addr = rsp.text

        # Check if we received a valid IPv4
        ipmatch = re.findall(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", addr)
        if len(ipmatch) > 0:
            out = addr
    except Exception as e:
        print("WARN: Could not get IP: %s" % str(e))

    return out


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

    localip = get_public_ip()
    if localip is None:
        localip = "0.0.0.0/0"
    else:
        print("INFO: Local IP: %s" % localip)
        localip = localip + "/32"

    sg.authorize_ingress(
        CidrIp=localip,
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
