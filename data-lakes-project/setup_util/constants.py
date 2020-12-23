DEFAULT_CONFIG: dict = {
    "Name": "MyCluster",
    "ReleaseLabel": "emr-6.0.0",
    "Instances": {
        "InstanceCount": 2,
        "MasterInstanceType": "m5.xlarge",
        "SlaveInstanceType": "m5.xlarge",
        "AdditionalMasterSecurityGroups": [],
        "Ec2KeyName": "US-West-2-key",
        "KeepJobFlowAliveWhenNoSteps": True
    },
    "Applications": [
        {"Name": "spark"},
        {"Name": "zeppelin"}
    ],
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole"
}

REGION: str = "us-west-2"

META_FILE: str = "cluster_meta.json"
