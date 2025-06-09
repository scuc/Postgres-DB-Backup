
## db backup and restore process using TablePlus

# Backup
 - select "backup" within TablePlus app
 - use TablePlus to connect to remote prod server
 - select the prod db from the list in TablePlus: ngceng_prod_db
 - use backup options: 
                --format=custom,
                --no-owner

# Restore

    - if the target db already exists, use psql to DROP the target db
    - select "restore" within TablePlus app
    - use TablePlus to connect to the target db (local or dev, etc)
    - select the 'db.dump' backup from its saved location
    - use restoe options: 
            --exit-on-error,
            --create

    - Note: when using 'create' target db cannot already exist, must be conencted to another db running in  
        the same postgres server. 
    - Note: restore status window should not show any errors. looking for only "database restore complete"