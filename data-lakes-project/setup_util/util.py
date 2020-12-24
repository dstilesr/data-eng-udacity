import os


def make_conn_string(
        master_addr: str,
        key_file: str = None,
        template: str = "connect.txt"):
    """
    Makes the connection string to connect to the cluster's master node.
    :param master_addr: DNS address for master node.
    :param key_file: Path to key file for SSH.
    :param template: Template for the string. The <master> pattern will be
        replaced by master_addr, and the <key-file> pattern will be replaced
        by key_file if one is given.
    :return: None. String is stored to a connection.bash file.
    """
    if not os.path.isfile(template):
        raise FileNotFoundError("No template found!")

    with open(template, "r") as f1, open("connection.bash", "w") as f2:
        content = f1.read()
        content = content.replace("<master>", master_addr)
        if key_file is not None:
            content = content.replace("<key-file>", key_file)

        f2.write(content)
    print("INFO: Wrote connection string to connection.bash file.")
