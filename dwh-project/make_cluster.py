import os
import sys
import json
import boto3
import configparser
from time import sleep


ROLE_NAME = "redshift-s3-role"
CLUSTER_ID = "sparkifyDWHCluster"


def make_role() -> str:
    """
    Makes an IAM role to allow the cluster to read from S3.
    :return: The Role's ARN
    """
    iam = boto3.client("iam")
    iam.create_role(
        RoleName=ROLE_NAME,
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [
                {'Action': 'sts:AssumeRole',
                 'Effect': 'Allow',
                 'Principal': {'Service': 'redshift.amazonaws.com'}
                 }
            ],
             'Version': '2012-10-17'}
        )
    )

    iam.attach_role_policy(
        RoleName=ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )
    print("INFO: Role Created.")
    return iam.get_role(RoleName=ROLE_NAME)["Role"]["Arn"]


def get_cluster_endpoint(
        redshift_client,
        max_tries: int = 24,
        interval: int = 30) -> str:
    """
    Waits for the cluster to become available, then gets the endpoint.
    :param redshift_client: Boto3 redshift client.
    :param max_tries: Maximum number of requests.
    :param interval: Interval between requests (seconds).
    :return: The cluster's endpoint address.
    """
    endpoint = None
    for _ in range(max_tries):
        properties = redshift_client.describe_clusters(
            ClusterIdentifier=CLUSTER_ID
        )['Clusters'][0]
        if properties["ClusterStatus"] == "available":
            print("INFO: Cluster startup complete.")
            endpoint = properties["Endpoint"]["Address"]
            print("INFO: Cluster endpoint: %s" % endpoint)
            break
        else:
            print("INFO: Wait %d seconds. . ." % interval)
            sleep(interval)

    if endpoint is None:
        raise ValueError("Cluster not started!")
    return endpoint


def setup_cluster(config):
    """
    Sets up the redshift cluster.
    :param config: Config variables dictionary.
    :return:
    """
    redshift = boto3.client("redshift", region_name="us-west-2")
    role_arn = make_role()
    redshift.create_cluster(
        DBName=config["CLUSTER"]["DB_NAME"],
        ClusterType="multi-node",
        NumberOfNodes=2,
        NodeType="dc2.large",

        ClusterIdentifier=CLUSTER_ID,
        MasterUsername=config["CLUSTER"]["DB_USER"],
        MasterUserPassword=config["CLUSTER"]["DB_PASSWORD"],
        VpcSecurityGroupIds=[config["SG"]["SG_ID"]],  # I had made this SG ahead of time

        IamRoles=[role_arn]
    )
    endpoint = get_cluster_endpoint(redshift)

    with open("cluster_meta.json", "w") as f:
        json.dump(
            {
                "RoleARN": role_arn,
                "ClusterEndpoint": endpoint
            },
            f
        )


def delete_cluster():
    """
    Deletes the redshift cluster.
    :return:
    """
    redshift = boto3.client("redshift", region_name="us-west-2")
    redshift.delete_cluster(
        ClusterIdentifier=CLUSTER_ID,
        SkipFinalClusterSnapshot=True
    )

    iam = boto3.client("iam")
    iam.detach_role_policy(
        RoleName=ROLE_NAME,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
    )
    iam.delete_role(RoleName=ROLE_NAME)

    os.remove("cluster_meta.json")


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    try:
        if len(sys.argv) > 1 and sys.argv[1] == "delete":
            print("INFO: Deleting Cluster.")
            delete_cluster()
        else:
            print("INFO: Creating Cluster.")
            setup_cluster(config)

        sys.exit(0)

    except Exception as e:
        print("ERROR: Task failed.")
        print(e)
        sys.exit(1)

