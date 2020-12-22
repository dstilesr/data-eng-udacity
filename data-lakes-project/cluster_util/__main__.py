import sys
import json
from . import cluster_setup, util, constants


try:
    if len(sys.argv) > 1 and sys.argv[1] == "delete":
        print("INFO: Deleting cluster")
        with open(constants.META_FILE, "r") as f:
            meta = json.load(f)

        cluster_setup.delete_cluster(meta["ClusterId"], meta["SGId"])
    else:
        meta = cluster_setup.launch_cluster()
        util.make_conn_string(
            meta["MasterNodeAddr"],
            "US-West-2-key.pem"
        )
    sys.exit(0)

except Exception as e:
    print("ERROR: Task Failed!")
    print(e)
    sys.exit(1)
