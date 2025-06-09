import logging
import logging.config
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path
from time import localtime, strftime

import yaml

import config as cfg
from backup_prod_db import (
    DatabaseBackupError,
    DatabaseRestoreError,
    alter_db_owner,
    disconnect_users,
    log_exception,
)
from backup_prod_db import backup_database as bkp_db
from backup_prod_db import recreate_target_db as recreate_db
from backup_prod_db import restore_database as restore_db

config = cfg.get_config()
script_root = config["script_root_path"]
db_name = config["db_name"]

logger = logging.getLogger(__name__)


class BackupRestoreProcessError(Exception):
    """Custom exception for the overall backup/restore process"""

    pass


def set_logger():
    """
    Setup logging configuration with enhanced error handling
    """
    try:
        path = os.path.join(script_root, "logging.yaml")

        if not os.path.exists(path):
            print(f"ERROR: Logging configuration file not found: {path}")
            sys.exit(1)

        with open(path, "rt") as f:
            config = yaml.safe_load(f.read())

            # get the file name from the handlers, append the date to the filename.
            for i in config["handlers"].keys():
                if "filename" in config["handlers"][i]:
                    log_filename = config["handlers"][i]["filename"]
                    base, extension = os.path.splitext(log_filename)
                    today = datetime.today()

                    log_filename = "{}_{}{}".format(
                        base, today.strftime("%Y%m%d"), extension
                    )
                    config["handlers"][i]["filename"] = log_filename

                    # Ensure log directory exists
                    log_dir = Path(log_filename).parent
                    log_dir.mkdir(parents=True, exist_ok=True)
                else:
                    continue

            logging.config.dictConfig(config)

        return True

    except yaml.YAMLError as e:
        print(f"ERROR: Invalid YAML in logging configuration: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Failed to setup logging: {e}")
        sys.exit(1)


def validate_environment():
    """Validate that all required environment variables and configs are set"""
    required_configs = [
        "db_name",
        "db_owner",
        "db_admin",
        "db_host",
        "db_port",
        "script_root_path",
    ]
    missing_configs = []

    for config_key in required_configs:
        if config_key not in config or not config[config_key]:
            missing_configs.append(config_key)

    if missing_configs:
        error_msg = (
            f"Missing required configuration values: {', '.join(missing_configs)}"
        )
        logger.error(error_msg)
        raise BackupRestoreProcessError(error_msg)

    logger.info("Environment validation passed")


def cleanup_old_backups(backup_dir, keep_days=365):
    """Clean up old backup files to save disk space"""
    try:
        backup_path = Path(backup_dir)
        if not backup_path.exists():
            logger.info("Backup directory does not exist, skipping cleanup")
            return

        cutoff_time = datetime.now().timestamp() - (keep_days * 24 * 3600)
        cleaned_count = 0

        for backup_file in backup_path.glob("*.sql"):
            if backup_file.stat().st_mtime < cutoff_time:
                file_size = backup_file.stat().st_size
                backup_file.unlink()
                logger.info(
                    f"Cleaned up old backup: {backup_file.name} ({file_size / (1024**2):.2f} MB)"
                )
                cleaned_count += 1

        if cleaned_count > 0:
            logger.info(f"Cleaned up {cleaned_count} old backup files")
        else:
            logger.info("No old backup files to clean up")

    except Exception as e:
        logger.warning(f"Failed to clean up old backups: {e}")


def send_notification(success, operation, error_details=None):
    """Send notification about backup/restore status (placeholder for email/slack/etc)"""
    try:
        status = "SUCCESS" if success else "FAILED"
        subject = f"Database {operation} {status} - {db_name}"

        if success:
            message = f"Database {operation} completed successfully for {db_name}"
            logger.info(f"NOTIFICATION: {subject}")
        else:
            message = f"Database {operation} failed for {db_name}"
            if error_details:
                message += f"\n\nError Details:\n{error_details}"
            logger.error(f"NOTIFICATION: {subject}")
            logger.error(f"Message: {message}")

        # TODO: Implement actual notification (email, Slack, etc.)
        # Example: send_email(subject, message)
        # Example: send_slack_message(subject, message)

    except Exception as e:
        logger.error(f"Failed to send notification: {e}")


def main():
    """
    Enhanced main function with comprehensive error handling and recovery options.

    This function performs the DB Backup and Restore process with detailed logging,
    error handling, and cleanup operations.

    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    operation_start_time = datetime.now()
    date_start = operation_start_time.strftime("%A, %d. %B %Y %I:%M%p")

    start_msg = f"""
    ================================================================
                DB Backup and Restore - Start
                    {date_start}
                Database: {db_name}
    ================================================================"""

    logger.info(start_msg)

    # Track individual operation status
    operations_status = {
        "environment_validation": False,
        "backup": False,
        "disconnect_users": False,
        "recreate_db": False,
        "restore": False,
        "alter_owner": False,
    }

    backup_file = None
    backup_path = None

    try:
        # Environment validation
        logger.info("Step 1/6: Validating environment")
        validate_environment()
        operations_status["environment_validation"] = True
        logger.info("✓ Environment validation completed")

        # Prepare backup paths
        backup_file = db_name + "_" + strftime("%Y%m%d%H%M%S") + ".sql"
        backup_path = os.path.join("_db_backups", backup_file)

        logger.info(f"Backup will be saved as: {backup_path}")

        # Step 1: Database Backup
        logger.info("Step 2/6: Starting database backup")
        bkp_db(backup_file, backup_path)
        operations_status["backup"] = True
        logger.info("✓ Database backup completed")

        # Step 2: Disconnect Users
        logger.info("Step 3/6: Disconnecting active users")
        disconnect_users()
        operations_status["disconnect_users"] = True
        logger.info("✓ User disconnection completed")

        # Step 3: Recreate Database
        logger.info("Step 4/6: Recreating target database")
        recreate_db()
        operations_status["recreate_db"] = True
        logger.info("✓ Database recreation completed")

        # Step 4: Restore Database
        logger.info("Step 5/6: Restoring database from backup")
        restore_db(backup_path)
        operations_status["restore"] = True
        logger.info("✓ Database restore completed")

        # Step 5: Alter Database Owner
        logger.info("Step 6/6: Setting database ownership")
        alter_db_owner()
        operations_status["alter_owner"] = True
        logger.info("✓ Database ownership configuration completed")

        # Cleanup old backups
        logger.info("Performing backup cleanup")
        cleanup_old_backups("_db_backups", keep_days=7)

        # Calculate duration
        operation_end_time = datetime.now()
        duration = operation_end_time - operation_start_time
        date_end = operation_end_time.strftime("%A, %d. %B %Y %I:%M%p")

        complete_msg = f"""
    ================================================================
                DB Backup and Restore - SUCCESS
                    Start: {date_start}
                    End: {date_end}
                    Duration: {duration}
                    Database: {db_name}
                    Backup File: {backup_file}
    ================================================================
        """
        logger.info(complete_msg)

        # Send success notification
        send_notification(True, "Backup and Restore")

        return 0

    except (DatabaseBackupError, DatabaseRestoreError, BackupRestoreProcessError) as e:
        # These are our custom exceptions with detailed logging already done
        error_summary = f"Database operation failed: {e}"
        logger.error(error_summary)

        # Log which operations succeeded and which failed
        logger.error("Operation Status Summary:")
        for operation, status in operations_status.items():
            status_symbol = "✓" if status else "✗"
            logger.error(f"  {status_symbol} {operation.replace('_', ' ').title()}")

        send_notification(False, "Backup and Restore", str(e))
        return 1

    except Exception as e:
        # Unexpected exceptions
        operation_name = "Main Backup/Restore Process"
        error_details = log_exception(
            operation_name,
            e,
            {
                "operations_status": operations_status,
                "backup_file": backup_file,
                "backup_path": backup_path,
            },
        )

        logger.error("CRITICAL: Unexpected error in main process")
        logger.error("Operation Status Summary:")
        for operation, status in operations_status.items():
            status_symbol = "✓" if status else "✗"
            logger.error(f"  {status_symbol} {operation.replace('_', ' ').title()}")

        send_notification(False, "Backup and Restore", f"Unexpected error: {e}")
        return 1

    finally:
        # Always log the final duration
        try:
            final_time = datetime.now()
            total_duration = final_time - operation_start_time
            logger.info(f"Total process duration: {total_duration}")

            # Log system resources if available
            try:
                import psutil

                memory_usage = psutil.virtual_memory().percent
                disk_usage = psutil.disk_usage("/").percent
                logger.info(
                    f"System resources at completion - Memory: {memory_usage}%, Disk: {disk_usage}%"
                )
            except ImportError:
                pass  # psutil not available

        except Exception as e:
            logger.error(f"Error in finally block: {e}")


if __name__ == "__main__":
    try:
        set_logger()
        exit_code = main()
        sys.exit(exit_code)

    except Exception as e:
        print(f"CRITICAL ERROR: Failed to initialize application: {e}")
        print(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)
