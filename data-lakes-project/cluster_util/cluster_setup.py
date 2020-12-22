import os
import json
import boto3
from time import sleep
from .sg_setup import make_sg, delete_sg
from .constants import DEFAULT_CONFIG, REGION, META_FILE


def get_emr_client(region: str = None):
    """
    Makes a boto3 EMR client.
    :param region: AWS region name.
    :return:
    """
    if region is None:
        region = REGION
    return boto3.client("emr", region_name=region)


def wait_init(
        cluster_id: str,
        max_tries: int = 24,
        wait_time: int = 30) -> str:
    """
    Waits for the cluster to initialize, then returns the address of the
    master node.
    :param cluster_id: ID of the cluster.
    :param max_tries:
    :param wait_time:
    :return: Address (DNS) of the Master Node.
    """
    emr = get_emr_client()
    addr: str = None

    for _ in range(max_tries):
        properties = emr.describe_cluster(ClusterId=cluster_id)["Cluster"]
        status = properties["Status"]["State"]

        if status in {"RUNNING", "WAITING"}:
            print("INFO: Cluster ready (Status: %s)" % status)
            addr = properties["MasterPublicDnsName"]
            break
        else:
            print("INFO: Waiting for cluster setup (%d seconds)" % wait_time)
            sleep(wait_time)

    if addr is None:
        raise ValueError("Cluster not started!")

    return addr


def launch_cluster(**overrides) -> dict:
    """
    Quickly launches an EMR cluster.
    :param overrides: kwargs to override values in DEFAULT_CONFIG.
    :return: Basic cluster info (Id, Master DNS name, SG Id)
    """
    if os.path.isfile(META_FILE):
        raise FileExistsError("Cluster already exists!")

    config = DEFAULT_CONFIG.copy()
    config.update(**overrides)

    sg = make_sg()
    config["Instances"].update(AdditionalMasterSecurityGroups=[sg.id])
    emr = get_emr_client()

    response = emr.run_job_flow(**config)
    cluster_id = response["JobFlowId"]
    master_addr = wait_init(cluster_id)

    meta = {
        "MasterNodeAddr": master_addr,
        "ClusterId": cluster_id,
        "SGId": sg.id
    }
    with open(META_FILE, "w") as f:
        json.dump(meta, f)

    print("INFO: Cluster Launched!")
    return meta


def delete_cluster(cluster_id: str, sg_id: str = None):
    """
    Terminates the given EMR cluster.
    :param cluster_id: ID of the cluster.
    :param sg_id: Id of additional Security Group.
    :return:
    """
    print("INFO: Deleting cluster %s" % cluster_id)
    emr = get_emr_client()
    emr.terminate_job_flows(JobFlowIds=[cluster_id])
    print("INFO: Cluster deleted.")

    if sg_id is not None:
        delete_sg(sg_id)

    os.remove(META_FILE)
