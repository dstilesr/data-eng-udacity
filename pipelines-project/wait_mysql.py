import os
from time import sleep
from sqlalchemy import create_engine


MAX_RETRIES = 10  #: Max number of connection tries


def try_connection(sqlalchemy_url: str) -> bool:
    """
    Tests whether it can connect to the mysql database
    :param sqlalchemy_url:
    :return: True if it can connect, False otherwise
    """
    engine = create_engine(
        sqlalchemy_url,
        connect_args={"connect_timeout": 10}
    )
    try:
        connect = engine.connect()
        connect.close()
        print("CONNECTION SUCCESFUL")
        return True

    except Exception as e:
        print(f"FAILED TO CONNECT: {str(e)[:300]}")
        return False


if __name__ == "__main__":

    for i in range(MAX_RETRIES):
        success = try_connection(os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN"))
        if success:
            break
        else:
            sleep(10)

    exit(0)
