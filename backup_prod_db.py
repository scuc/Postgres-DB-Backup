import logging
import os
import subprocess
import sys
import traceback
from pathlib import Path

import psycopg2
from psycopg2 import DatabaseError, OperationalError

import config as cfg

config = cfg.get_config()

db_name = config["db_name"]
db_owner = config["db_owner"]
db_admin = config["db_admin"]
# db_admin_pw: config["db_admin_pw"]
db_password = config["db_password"]
db_host = config["db_host"]
db_port = config["db_port"]
# backup_path = config["backup_path"]
root_path = config["script_root_path"]
db_dev_name = config["db_dev_name"]


logger = logging.getLogger(__name__)


class DatabaseBackupError(Exception):
    """Custom exception for database backup operations"""

    pass


class DatabaseRestoreError(Exception):
    """Custom exception for database restore operations"""

    pass


def log_exception(operation_name, exception, additional_context=None):
    """
    Comprehensive exception logging with full traceback and context

    Args:
        operation_name (str): Name of the operation that failed
        exception (Exception): The exception that was raised
        additional_context (dict): Additional context information
    """
    error_details = {
        "operation": operation_name,
        "exception_type": type(exception).__name__,
        "exception_message": str(exception),
        "traceback": traceback.format_exc(),
    }

    if additional_context:
        error_details.update(additional_context)

    logger.error(f"OPERATION FAILED: {operation_name}")
    logger.error(f"Exception Type: {error_details['exception_type']}")
    logger.error(f"Exception Message: {error_details['exception_message']}")

    if additional_context:
        logger.error(f"Additional Context: {additional_context}")

    logger.error(f"Full Traceback:\n{error_details['traceback']}")

    return error_details


def connect(db_name):
    """Open a connection the the postgres db with enhanced error handling."""
    operation_name = f"Database Connection to {db_name}"

    try:
        logger.info(f"Attempting to connect to database: {db_name}")
        logger.debug(
            f"Connection parameters - Host: {db_host}, Port: {db_port}, User: {db_owner}"
        )

        db = psycopg2.connect(dbname=db_name, user=db_owner, host=db_host, port=db_port)
        cursor = db.cursor()

        logger.info(f"Successfully connected to database: {db_name}")
        return db, cursor

    except OperationalError as e:
        context = {
            "database": db_name,
            "host": db_host,
            "port": db_port,
            "user": db_owner,
        }
        log_exception(operation_name, e, context)
        raise DatabaseBackupError(f"Failed to connect to database {db_name}: {e}")

    except Exception as e:
        log_exception(operation_name, e)
        raise DatabaseBackupError(
            f"Unexpected error connecting to database {db_name}: {e}"
        )


def validate_backup_prerequisites(backup_path):
    """Validate that backup can proceed"""
    # Check if backup directory exists
    backup_dir = Path(backup_path).parent
    if not backup_dir.exists():
        logger.info(f"Creating backup directory: {backup_dir}")
        backup_dir.mkdir(parents=True, exist_ok=True)

    # Check disk space (basic check)
    import shutil

    free_space = shutil.disk_usage(backup_dir).free
    logger.info(f"Available disk space: {free_space / (1024**3):.2f} GB")

    if free_space < 1024**3:  # Less than 1GB
        logger.warning("Low disk space detected - backup may fail")


def backup_database(backup_file, backup_path):
    """Use pg_dump to take a full backup of the production db with enhanced error handling"""
    operation_name = f"Database Backup of {db_name}"

    try:
        logger.info(f"Starting backup operation for database: {db_name}")
        logger.info(f"Backup file: {backup_file}")
        logger.info(f"Backup path: {backup_path}")

        # Validate prerequisites
        validate_backup_prerequisites(backup_path)

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

        logger.info(f"Executing pg_dump command: {' '.join(cmd_str)}")

        pgdump_process = subprocess.run(
            cmd_str,
            capture_output=True,
            check=True,
            text=True,
            timeout=300,  # Increased timeout to 5 minutes
        )

        log_captured_output(pgdump_process)

        if pgdump_process.returncode != 0:
            error_msg = f"pg_dump exited with return code {pgdump_process.returncode}"
            logger.error(error_msg)
            logger.error(f"STDERR: {pgdump_process.stderr}")
            raise DatabaseBackupError(error_msg)

        # Verify backup file was created and has reasonable size
        backup_file_path = Path(backup_path)
        if not backup_file_path.exists():
            raise DatabaseBackupError(f"Backup file was not created: {backup_path}")

        file_size = backup_file_path.stat().st_size
        logger.info(
            f"Backup file created successfully. Size: {file_size / (1024**2):.2f} MB"
        )

        if file_size < 1024:  # Less than 1KB seems suspicious
            logger.warning(
                f"Backup file size is very small ({file_size} bytes) - this may indicate an issue"
            )

        logger.info("Database backup completed successfully")
        return True

    except subprocess.TimeoutExpired as e:
        context = {
            "database": db_name,
            "timeout_seconds": 300,
            "command": " ".join(cmd_str),
        }
        log_exception(operation_name, e, context)
        raise DatabaseBackupError(f"Backup operation timed out after 5 minutes")

    except subprocess.CalledProcessError as e:
        context = {
            "database": db_name,
            "return_code": e.returncode,
            "command": " ".join(cmd_str),
            "stdout": e.stdout,
            "stderr": e.stderr,
        }
        log_exception(operation_name, e, context)
        raise DatabaseBackupError(f"pg_dump failed with return code {e.returncode}")

    except Exception as e:
        context = {"database": db_name, "backup_path": backup_path}
        log_exception(operation_name, e, context)
        raise DatabaseBackupError(f"Unexpected error during backup: {e}")


def disconnect_users():
    """Disconnect all public users from the DB with enhanced error handling"""
    operation_name = f"Disconnect Users from {db_name}"
    db = None
    cursor = None

    try:
        logger.info(f"Starting user disconnection for database: {db_name}")

        db, cursor = connect(db_name)

        # First, get count of active connections
        count_query = f"SELECT count(*) FROM pg_stat_activity WHERE datname='{db_name}' AND pid <> pg_backend_pid() AND leader_pid IS NULL;"
        cursor.execute(count_query)
        active_connections = cursor.fetchone()[0]

        logger.info(f"Found {active_connections} active connections to terminate")

        if active_connections == 0:
            logger.info("No active connections to terminate")
            return True

        # Terminate connections
        terminate_query = f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname='{db_name}' AND leader_pid IS NULL;"

        logger.info("Executing pg_terminate_backend for all active connections")
        cursor.execute(terminate_query)
        terminated_results = cursor.fetchall()

        successful_terminations = sum(1 for result in terminated_results if result[0])
        logger.info(f"Successfully terminated {successful_terminations} connections")

        # Verify no connections remain
        cursor.execute(count_query)
        remaining_connections = cursor.fetchone()[0]

        if remaining_connections > 0:
            logger.warning(
                f"{remaining_connections} connections still active after termination attempt"
            )
        else:
            logger.info("All connections successfully terminated")

        return True

    except (OperationalError, DatabaseError) as e:
        context = {
            "database": db_name,
            "active_connections": locals().get("active_connections", "unknown"),
        }
        log_exception(operation_name, e, context)
        raise DatabaseBackupError(f"Database error during user disconnection: {e}")

    except Exception as e:
        log_exception(operation_name, e)
        raise DatabaseBackupError(f"Unexpected error during user disconnection: {e}")

    finally:
        if cursor:
            cursor.close()
        if db:
            db.close()


def recreate_target_db():
    """Drop existing DB and create a new empty copy with enhanced error handling"""
    operation_name = f"Recreate Database {db_name}"
    db = None
    cursor = None

    try:
        logger.info(f"Starting database recreation for: {db_name}")

        # Connect to a different database to drop/create the target
        db, cursor = connect(db_dev_name)
        db.autocommit = True

        # Check if database exists first
        check_query = f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'"
        cursor.execute(check_query)
        db_exists = cursor.fetchone() is not None

        if db_exists:
            logger.info(f"Database {db_name} exists, proceeding with drop")
            dropdb_query = f"DROP DATABASE {db_name};"
            cursor.execute(dropdb_query)
            logger.info(f"Database {db_name} dropped successfully")
        else:
            logger.info(f"Database {db_name} does not exist, skipping drop")

        createdb_query = f"CREATE DATABASE {db_name} OWNER {db_owner};"
        cursor.execute(createdb_query)
        logger.info(f"Database {db_name} created successfully with owner {db_owner}")

        return True

    except (OperationalError, DatabaseError) as e:
        context = {
            "target_database": db_name,
            "admin_database": db_dev_name,
            "database_owner": db_owner,
        }
        log_exception(operation_name, e, context)
        raise DatabaseBackupError(f"Database error during recreation: {e}")

    except Exception as e:
        log_exception(operation_name, e)
        raise DatabaseBackupError(f"Unexpected error during database recreation: {e}")

    finally:
        if cursor:
            cursor.close()
        if db:
            db.close()


def restore_database(backup_path):
    """Restore database from backup with enhanced error handling"""
    operation_name = f"Database Restore of {db_name}"

    try:
        logger.info(f"Starting database restore for: {db_name}")
        logger.info(f"Restore source: {backup_path}")

        # Verify backup file exists and is readable
        backup_file_path = Path(backup_path)
        if not backup_file_path.exists():
            raise DatabaseRestoreError(f"Backup file does not exist: {backup_path}")

        if not backup_file_path.is_file():
            raise DatabaseRestoreError(f"Backup path is not a file: {backup_path}")

        file_size = backup_file_path.stat().st_size
        logger.info(f"Backup file size: {file_size / (1024**2):.2f} MB")

        # Schema restore
        cmd_str_schema = [
            "pg_restore",
            "-v",
            "-e",
            f"-h{db_host}",
            f"-U{db_owner}",
            "--schema-only",
            "--single-transaction",
            f"--role={db_owner}",
            f"--dbname={db_name}",
            backup_path,
        ]

        logger.info("Starting schema restore")
        logger.info(f"Schema restore command: {' '.join(cmd_str_schema)}")

        pgrestore_schema = subprocess.run(
            cmd_str_schema,
            capture_output=True,
            check=True,
            text=True,
            timeout=300,
        )

        log_captured_output(pgrestore_schema)

        if pgrestore_schema.returncode != 0:
            error_msg = (
                f"Schema restore failed with return code {pgrestore_schema.returncode}"
            )
            logger.error(error_msg)
            raise DatabaseRestoreError(error_msg)

        logger.info("Schema restore completed successfully")

        # Data restore
        cmd_str_data = [
            "pg_restore",
            "-v",
            "-e",
            f"-h{db_host}",
            f"-U{db_owner}",
            "--data-only",
            "--single-transaction",
            "--exit-on-error",
            f"--role={db_owner}",
            f"--dbname={db_name}",
            backup_path,
        ]

        logger.info("Starting data restore")
        logger.info(f"Data restore command: {' '.join(cmd_str_data)}")

        pgrestore_data = subprocess.run(
            cmd_str_data,
            capture_output=True,
            check=True,
            text=True,
            timeout=600,  # 10 minutes for data restore
        )

        log_captured_output(pgrestore_data)

        if pgrestore_data.returncode != 0:
            error_msg = (
                f"Data restore failed with return code {pgrestore_data.returncode}"
            )
            logger.error(error_msg)
            raise DatabaseRestoreError(error_msg)

        logger.info("Data restore completed successfully")
        logger.info("Full database restore completed successfully")
        return True

    except subprocess.TimeoutExpired as e:
        context = {
            "database": db_name,
            "backup_path": backup_path,
            "phase": "schema" if "schema" in str(e.cmd) else "data",
        }
        log_exception(operation_name, e, context)
        raise DatabaseRestoreError(f"Restore operation timed out: {e}")

    except subprocess.CalledProcessError as e:
        context = {
            "database": db_name,
            "backup_path": backup_path,
            "return_code": e.returncode,
            "command": " ".join(e.cmd) if e.cmd else "unknown",
            "stdout": e.stdout,
            "stderr": e.stderr,
        }
        log_exception(operation_name, e, context)
        raise DatabaseRestoreError(f"pg_restore failed with return code {e.returncode}")

    except Exception as e:
        context = {"database": db_name, "backup_path": backup_path}
        log_exception(operation_name, e, context)
        raise DatabaseRestoreError(f"Unexpected error during restore: {e}")


def alter_db_owner():
    """Alter database ownership with enhanced error handling"""
    operation_name = f"Alter DB Owner for {db_name}"
    db = None
    cursor = None

    try:
        logger.info(f"Starting ownership modification for database: {db_name}")

        db, cursor = connect(db_name)

        # Grant privileges
        query_db_privileges = (
            f"GRANT ALL PRIVILEGES ON DATABASE {db_name} TO {db_admin};"
        )
        logger.info(f"Granting all privileges to {db_admin}")
        cursor.execute(query_db_privileges)
        db.commit()
        logger.info(f"All privileges of db {db_name} granted to '{db_admin}'")

        # Alter owner
        query_db_owner = f"ALTER DATABASE {db_name} OWNER TO {db_owner};"
        logger.info(f"Changing database owner to {db_owner}")
        cursor.execute(query_db_owner)
        db.commit()
        logger.info(f"DB owner altered to '{db_owner}'")

        return True

    except (OperationalError, DatabaseError) as e:
        context = {
            "database": db_name,
            "target_owner": db_owner,
            "admin_user": db_admin,
        }
        log_exception(operation_name, e, context)
        raise DatabaseBackupError(f"Database error during ownership change: {e}")

    except Exception as e:
        log_exception(operation_name, e)
        raise DatabaseBackupError(f"Unexpected error during ownership change: {e}")

    finally:
        if cursor:
            cursor.close()
        if db:
            db.close()


def log_captured_output(captured_output):
    """Enhanced logging of subprocess output"""
    logger.info(f"Process Command: {' '.join(captured_output.args)}")
    logger.info(f"Process Return Code: {captured_output.returncode}")

    if captured_output.stdout:
        logger.info(f"Process STDOUT:\n{captured_output.stdout}")
    else:
        logger.info("Process STDOUT: (empty)")

    if captured_output.stderr:
        if captured_output.returncode == 0:
            logger.info(f"Process STDERR:\n{captured_output.stderr}")
        else:
            logger.error(f"Process STDERR:\n{captured_output.stderr}")
    else:
        logger.info("Process STDERR: (empty)")


if __name__ == "__main__":
    try:
        db, cursor = connect(db_name)
        logger.info("Connection test successful")
        cursor.close()
        db.close()
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        sys.exit(1)
