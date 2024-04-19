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
db_dev_name = config["db_dev_name"]


logger = logging.getLogger(__name__)


def connect(db_name):
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

    db, cursor = connect(db_name)
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


def recreate_target_db():
    """Drop existing DB and create a new empty copy to restore backup into"""

    try:
        db, cursor = connect(db_dev_name)

        db.autocommit = True

        dropdb_query = f"DROP DATABASE {db_name};"
        createdb_query = f"CREATE DATABASE {db_name}"

        cursor.execute(dropdb_query)
        cursor.execute(createdb_query)

        logger.info(f"DB Drop and Create executed: {db_name}")

    except Exception as e:
        logger.error(f"DB Drop/Create failed with error: {e}")

    return


def restore_database(backup_path):
    """Restore most recent backup of the pg db"""

    cmd_str_schema = [
        "pg_restore",
        "-v",
        "-e",
        f"-U{db_owner}",
        "--schema-only",
        "--single-transaction",
        f"--role={db_owner}",
        f"--dbname={db_name}",
        backup_path,
    ]

    cmd_str_data = [
        "pg_restore",
        "-v",
        "-e",
        f"-U{db_owner}",
        "--data-only",
        "--single-transaction",
        "--exit-on-error",
        f"--role={db_owner}",
        f"--dbname={db_name}",
        backup_path,
    ]

    try:
        logger.info(f"Attempting pg_restore of db: {db_name}")
        pgrestore_schema = subprocess.run(
            cmd_str_schema,
            capture_output=True,
            check=True,
            text=True,
            timeout=120,
        )

        log_captured_output(pgrestore_schema)

        if pgrestore_schema.returncode != 0:
            logger.error(
                "pg_restore exited with non-zero value, db restore not complete."
            )
        else:
            logger.info("pg_restore for schema-only is complete.")

            pgrestore_data = subprocess.run(
                cmd_str_data,
                capture_output=True,
                check=True,
                text=True,
                timeout=120,
            )

            if pgrestore_data.returncode != 0:
                logger.error(
                    "pg_restore exited with non-zero value, db restore not complete."
                )
            else:
                logger.info("pg_restore for data-only is complete.")

    except Exception as e:
        excp_msg = f"Exception raised during db restore: \n {e}\n"
        logger.error(excp_msg)
        print(excp_msg)

    return


def alter_db_owner():
    db, cursor = connect(db_name)

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
