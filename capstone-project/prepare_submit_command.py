import os
from dotenv import load_dotenv

load_dotenv(".env")

VARS_LIST = [
    "BUCKET_NAME",
    "STORM_DATA_PATH",
    "OUTPUT_PATH",
    "TEMP_DATA_FILE"
]

if __name__ == '__main__':
    """
    Fill in the submit command template to include the necessary environment
    variables.
    """
    with open("spark_submit_template.txt", "r") as f:
        template = f.read()

    envs = []
    for k in VARS_LIST:
        cmd = f'--conf "spark.executorEnv.{k}={os.getenv(k)}"'
        envs.append(cmd)

    out = template.format(env_conf=" ".join(envs))
    with open("submit_command.sh", "w") as f:
        f.write(out)
