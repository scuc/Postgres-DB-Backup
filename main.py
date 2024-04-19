import logging
import logging.config
import os
from datetime import datetime
from time import localtime, strftime

import yaml

import config as cfg
from backup_prod_db import alter_db_owner, disconnect_users
from backup_prod_db import backup_database as bkp_db
from backup_prod_db import recreate_target_db as recreate_db
from backup_prod_db import restore_database as restore_db

config = cfg.get_config()
script_root = config["script_root_path"]
db_name = config["db_name"]

logger = logging.getLogger(__name__)


def set_logger():
    """
    Setup logging configuration
    """
    path = os.path.join(script_root, "logging.yaml")

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
            else:
                continue

        logger = logging.config.dictConfig(config)

    return logger


def main():
    """
    This function performs the DB Backup and Restore process.

    It creates a backup of the database, disconnects users, recreates the database,
    restores the database from the backup, and alters the database owner.

    Returns:
        None
    """
    date_start = str(strftime("%A, %d. %B %Y %I:%M%p", localtime()))

    start_msg = f"\n\
    ================================================================\n\
                DB Backup and Restore - Start\n\
                    {date_start}\n\
    ================================================================"

    logger.info(start_msg)

    backup_file = db_name + "_" + strftime("%Y%m%d%H%M%S") + ".sql"
    backup_path = os.path.join("_db_backups", backup_file)

    bkp_db(backup_file, backup_path)
    disconnect_users()
    recreate_db()
    restore_db(backup_path)
    alter_db_owner()

    date_end = str(strftime("%A, %d. %B %Y %I:%M%p", localtime()))

    complete_msg = f"\n\
    ================================================================\n\
                DB Backup and Restore - Complete\n\
                    {date_end}\n\
    ================================================================\n\
    "
    logger.info(complete_msg)
    return


if __name__ == "__main__":
    set_logger()
    main()
