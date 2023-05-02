import os
from time import strftime

import psycopg2

import config as cfg

config = cfg.get_config()

db_name = config["db_name"]
db_owner = config["db_owner"]
db_admin = config["db_admin"]
db_password = config["db_password"]
db_host = config["db_host"]
db_port = config["db_port"]
backup_path = config["backup_path"]


def connect():
    """Open a connection the the postgres db."""
    try:
        db = psycopg2.connect(dbname=db_name, user=db_owner)
        cursor = db.cursor()
        return db, cursor
    except psycopg2.OperationalError as e:
        print(f"Unable to connect!\n error: {e}")


def backup_database(backup_file):
    """TODO: add docstring"""
    command_str = f"pg_dump -h {db_host} -U {db_owner} -d {db_name} --no-owner --format=custom -v > '{os.path.join(backup_path,backup_file)}'"

    try:
        os.system(command_str)
        print("Backup completed")
    except Exception as e:
        print("!!Problem occured!!")
        print(e)


def disconnect_users():
    """Disconnect all public users from the DB and return a confirmation."""
    db, cursor = connect()
    query = f"SELECT pg_terminate_backend(pid)FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname='{db_name}' AND leader_pid IS NULL;"
    try:
        cursor.execute(query)
        connected_users = cursor.execute("select * from pg_stat_activity;")
        print(f"Number of connected users = {connected_users}")
        return
    except Exception as e:
        print(f"error in discconect users: \n {e}")


def restore_database(backup_file):
    """Restore most recent backup of db"""

    command_str = f"pg_restore -v -e -U {db_owner} --clean --role {db_owner} --dbname {db_name} '{os.path.join(backup_path,backup_file)}';"
    try:
        disconnect_users()
        os.system(command_str)
        print("DB RESTORE COMPLETE")

        db, cursor = connect()

        # print(cursor.execute(f"SELECT * FROM slatedoc_slatedoc WHERE id=142896;"))

        query_db_privileges = (
            f"GRANT ALL PRIVILEGES ON DATABASE {db_name} to {db_admin};"
        )
        cursor.execute(query_db_privileges)
        db.commit()
        print("GRANT PRIV COMPLETE")

        query_db_owner = f"ALTER DATABASE {db_name} OWNER TO {db_owner};"
        cursor.execute(query_db_owner)
        db.commit()
        print("DB OWNER COMPLETE")

    except Exception as e:
        print("!!Problem occured!!")
        print(e)

    cursor.close()
    return


if __name__ == "__main__":
    connect()
