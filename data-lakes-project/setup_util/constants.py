DEFAULT_CONFIG: dict = {
    "Name": "MyCluster",
    "ReleaseLabel": "emr-6.0.0",
    "Instances": {
        "AdditionalMasterSecurityGroups": [],
        "Ec2KeyName": "US-West-2-key",
        "KeepJobFlowAliveWhenNoSteps": True,
        "InstanceGroups": [
            {
                "Name": "WorkersSpot",
                "Market": "SPOT",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 3,
                "InstanceRole": "CORE"
            },
            {
                "Name": "Master",
                "Market": "ON_DEMAND",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
                "InstanceRole": "MASTER"
            }
        ]
    },
    "Applications": [
        {"Name": "spark"}
    ],
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole"
}

REGION: str = "us-west-2"

META_FILE: str = "cluster_meta.json"
