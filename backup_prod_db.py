import logging
import logging.config
import subprocess

import psycopg2

import config as cfg

config = cfg.get_config()

db_name = config["db_name"]
db_owner = config["db_owner"]
db_admin = config["db_admin"]
db_admin_pw: config["db_admin_pw"]
db_password = config["db_password"]
db_host = config["db_host"]
db_port = config["db_port"]
# backup_path = config["backup_path"]
root_path = config["script_root_path"]


logger = logging.getLogger(__name__)


def connect():
    """Open a connection the the postgres db."""
    try:
        db = psycopg2.connect(dbname=db_name, user=db_owner)
        cursor = db.cursor()
        return db, cursor
    except psycopg2.OperationalError as e:
        excp_msg = f"Unable to connect!\n error: {e}"
        print(excp_msg)
        logger.error(excp_msg)


def backup_database(backup_file, backup_path):
    """Use pg_dump to take a full backup of the production db"""

    try:
        cmd_str = [
            "pg_dump",
            f"-h{db_host}",
            f"-U{db_admin}",
            f"-d{db_name}",
            "--no-owner",
            "--format=custom",
            "-v",
            f"-f{backup_path}",
        ]

        pgdump_process = subprocess.run(
            cmd_str,
            capture_output=True,
            check=True,
            text=True,
            timeout=120,
        )

        logger.info(f"Attempting to take a backup of db: {db_name}")

        log_captured_output(pgdump_process)

        if pgdump_process.returncode != 0:
            logger.error("pg_dump exited with non-zero value, db backup not complete.")
        else:
            logger.info("DB backup completed.")

    except Exception as e:
        excp_msg = f"Exception raised during the DB backup: \n {e}\n"
        print(excp_msg)
        logger.error(excp_msg)

    return


def disconnect_users():
    """Disconnect all public users from the DB and return a confirmation."""

    db, cursor = connect()
    query = f"SELECT pg_terminate_backend(pid)FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname='{db_name}' AND leader_pid IS NULL;"

    try:
        logger.info(f"pg_terminate all active db connections.")
        cursor.execute(query)

        connected_users = cursor.execute("select * from pg_stat_activity;")
        logger.info(f"Number of connected users = {connected_users}")

    except Exception as e:
        excp_msg = f"Exception raised during user disconnect: \n {e}\n"
        logger.error(excp_msg)
        print(excp_msg)

    return


def restore_database(backup_path):
    """Restore most recent backup of the pg db"""

    cmd_str = [
        "pg_restore",
        "-v",
        "-e",
        f"-U{db_owner}",
        "--clean",
        f"--role={db_owner}",
        f"--dbname={db_name}",
        backup_path,
    ]

    try:
        disconnect_users()
        logger.info(f"Attempting pg_restore of db: {db_name}")
        pgrestore_process = subprocess.run(
            cmd_str,
            capture_output=True,
            check=True,
            text=True,
            timeout=120,
        )

        log_captured_output(pgrestore_process)

        if pgrestore_process.returncode != 0:
            logger.error(
                "pg_restore exited with non-zero value, db restore not complete."
            )
        else:
            logger.info("pg_restore complete.")

    except Exception as e:
        excp_msg = f"Exception raised during db restore: \n {e}\n"
        logger.error(excp_msg)
        print(excp_msg)

    return


def alter_db_owner():
    db, cursor = connect()

    query_db_privileges = f"GRANT ALL PRIVILEGES ON DATABASE {db_name} to {db_admin};"
    cursor.execute(query_db_privileges)
    db.commit()
    logger.info(f"All privildges of db {db_name} granted to '{db_admin}'")

    query_db_owner = f"ALTER DATABASE {db_name} OWNER TO {db_owner};"
    cursor.execute(query_db_owner)
    db.commit()

    logger.info(f"DB owner altered to '{db_owner}'")

    cursor.close()


def log_captured_output(captured_output):
    logger.info(f"Process Args={captured_output.args}")
    logger.info(f"Process return code={captured_output.returncode}")
    logger.info(f"\nProcess STDOUT:\n{captured_output.stdout}")
    logger.info(f"\nProcess STDERR:\n{captured_output.stderr}")

    return


if __name__ == "__main__":
    connect()
