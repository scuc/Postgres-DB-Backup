
#!/bin/bash

set -e
set -u

DATE=(date +"%Y%m%d-%H%M%S")
PGDATABASE="ngceng_prod_db"

# Back up pg db to a file, labeled with datetime stamp.
BACKUP_FILE="postgres_${PGDATABASE}_${DATE}.sql"

cd /home/ngceng/scripts/postgres_db_backup
pipenv run python main.py
exit

