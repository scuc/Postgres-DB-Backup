# PostgreSQL Database Backup and Restore Script

A robust, production-ready Python script for backing up PostgreSQL databases from remote servers and restoring them to local environments with comprehensive error handling and cross-version compatibility.

## Features

### 🚀 Core Functionality
- **Remote to Local Backup/Restore**: Backup from remote PostgreSQL server and restore to local instance
- **Cross-Version Compatibility**: Handles PostgreSQL version differences (tested with PostgreSQL 13 → 16)
- **Smart SQL Filtering**: Automatically removes problematic statements for cross-version compatibility
- **Comprehensive Error Handling**: Detailed logging and exception handling with full tracebacks
- **Progress Tracking**: Step-by-step progress with clear success/failure indicators

### 🛡️ Reliability & Safety
- **Connection Validation**: Tests both remote and local database connections
- **Backup Verification**: Validates backup file creation and size
- **User Disconnection**: Safely terminates active database connections before operations
- **Database Recreation**: Clean drop/create cycle for target database
- **Ownership Management**: Proper database ownership and privilege configuration

### 📊 Monitoring & Logging
- **Structured Logging**: Timestamped logs with function names and line numbers
- **Operation Status Tracking**: Clear indication of which steps succeeded/failed
- **Performance Metrics**: Duration tracking and system resource monitoring
- **Backup Cleanup**: Automatic cleanup of old backup files
- **Notification System**: Placeholder for email/Slack notifications

### 🔧 Configuration & Security
- **YAML Configuration**: Clean, readable configuration management
- **`.pgpass` Support**: Secure password management using PostgreSQL standard
- **Environment Variables**: Alternative password configuration via `PGPASSWORD`
- **Flexible Database Settings**: Separate configuration for source and target databases

## Requirements

- Python 3.13+
- PostgreSQL client tools (`pg_dump`, `pg_restore`, `psql`)
- Network access to remote PostgreSQL server
- Local PostgreSQL installation

## Installation

1. **Clone or download the script files**:
   ```bash
   git clone <repository-url>
   cd postgres-db-backup
   ```

2. **Install dependencies using uv** (recommended):
   ```bash
   uv sync
   ```

   Or using pip:
   ```bash
   pip install psycopg2-binary pyyaml
   ```

3. **Ensure PostgreSQL client tools are available**:
   ```bash
   which pg_dump pg_restore psql
   ```

## Configuration

### 1. Create `config.yaml`

```yaml
# Remote Production Database (backup source)
db_name: "your_prod_db"
db_owner: "your_db_user"
db_admin: "your_db_admin"
db_host: "your.remote.server.com"
db_port: "5432"

# Local Database (restore target)
local_db_name: "your_local_db"
local_db_owner: "postgres"
local_db_admin: "postgres"
local_db_host: "localhost"
local_db_port: "5432"

# Script Settings
script_root_path: "/path/to/your/script/directory"
db_dev_name: "postgres"  # Admin database for local operations

# Optional Settings
backup_retention_days: 7
backup_directory: "_db_backups"
```

### 2. Setup Password Authentication

**Option A: Using `.pgpass` file (Recommended)**

Create `~/.pgpass` with proper permissions:
```bash
# Create .pgpass file
echo "your.remote.server.com:5432:your_prod_db:your_db_user:your_remote_password" > ~/.pgpass
echo "localhost:5432:*:postgres:your_local_password" >> ~/.pgpass
chmod 600 ~/.pgpass
```

**Option B: Using Environment Variable**
```bash
export PGPASSWORD="your_database_password"
```

### 3. Setup Logging

The script uses the included `logging.yaml` configuration. Logs are automatically rotated daily and stored in the `logs` directory.

## Usage

### Basic Usage

```bash
# Using uv (recommended)
uv run python main.py

# Or directly with Python
python main.py
```

### Testing Connections

Test database connections before running the full process:
```bash
python backup_prod_db.py
```

### Manual Backup Only

To create a backup without restore:
```bash
python -c "
from backup_prod_db import backup_database
backup_database('manual_backup.sql', '_db_backups/manual_backup.sql')
"
```

## Script Workflow

The script performs these operations in sequence:

1. **Environment Validation** ✓
   - Validates all required configuration values
   - Tests database connections

2. **Database Backup** ✓
   - Creates timestamped backup from remote database
   - Uses plain SQL format for maximum compatibility
   - Validates backup file creation and size

3. **User Disconnection** ✓
   - Safely terminates active connections to target database
   - Provides count of disconnected users

4. **Database Recreation** ✓
   - Drops existing target database (if exists)
   - Creates fresh empty database with correct ownership

5. **SQL Filtering** ✓
   - Removes problematic PostgreSQL version-specific statements
   - Maintains compatibility between different PostgreSQL versions

6. **Database Restore** ✓
   - Restores filtered backup to local database using `psql`
   - Comprehensive error handling for restore issues

7. **Ownership Configuration** ✓
   - Sets proper database ownership and privileges
   - Configures access permissions

8. **Cleanup** ✓
   - Removes old backup files (configurable retention)
   - Logs final statistics and duration

## Cross-Version Compatibility

The script automatically handles PostgreSQL version differences by filtering out problematic statements:

- `SET transaction_timeout = 0;`
- `SET idle_in_transaction_session_timeout = 0;`
- `SET row_security = off;`
- `SET xmloption = content;`
- `SET default_table_access_method = heap;`

This enables seamless backup/restore between different PostgreSQL versions (e.g., PostgreSQL 13 → PostgreSQL 16).

## Logging and Monitoring

### Log Files

- **Info Logs**: `logs/info_YYYYMMDD.log` - All operations and progress
- **Error Logs**: `logs/errors_YYYYMMDD.log` - Errors and exceptions only
- **Console Output**: Real-time progress and status updates

### Log Format

```
2025-06-09 12:55:53,181 | INFO | Function: main() | Line 180 |
    ================================================================
                DB Backup and Restore - Start
                    Monday, 09. June 2025 12:55PM
                Database: your_prod_db
    ================================================================
```

### Success Indicators

- ✅ **All Steps Completed**: All 6 steps show checkmarks
- 🚀 **Performance**: Typical completion time 3-5 seconds
- 📊 **Statistics**: Backup size, duration, and system resources logged

## Error Handling

### Exception Types

- **`DatabaseBackupError`**: Issues with backup operations
- **`DatabaseRestoreError`**: Issues with restore operations
- **`BackupRestoreProcessError`**: Overall process failures

### Recovery Information

Each error includes:
- **Operation Context**: What operation was being performed
- **Full Traceback**: Complete Python stack trace
- **Additional Context**: Database names, file paths, command details
- **Status Summary**: Which operations succeeded vs. failed

### Common Issues and Solutions

#### 1. Connection Refused
```
connection to server at "localhost" failed: Connection refused
```
**Solution**: Start local PostgreSQL service
```bash
brew services start postgresql@15  # macOS with Homebrew
sudo systemctl start postgresql    # Linux
```

#### 2. Authentication Failed
```
FATAL: password authentication failed for user "username"
```
**Solution**: Check `.pgpass` file format and permissions
```bash
chmod 600 ~/.pgpass
cat ~/.pgpass  # Verify format: host:port:database:username:password
```

#### 3. Cross-Version Issues
```
ERROR: unrecognized configuration parameter "transaction_timeout"
```
**Solution**: The script automatically filters these statements. If you see this error, the filtering may need updates.

## File Structure

```
postgres-db-backup/
├── main.py                 # Main script entry point
├── backup_prod_db.py       # Core backup/restore functions
├── config.py              # Configuration management
├── config.yaml            # Database and script configuration
├── logging.yaml           # Logging configuration
├── pyproject.toml         # Python project configuration
├── uv.lock               # Dependency lock file
├── logs/                # Log files (auto-created)
│   ├── info_20250609.log
│   └── errors_20250609.log
├── _db_backups/          # Backup files (auto-created)
│   ├── your_db_20250609123456.sql
│   └── your_db_20250609123456_filtered.sql
└── README.md             # This file
```

## Advanced Configuration

### Custom Backup Retention

Modify the cleanup period in `main.py`:
```python
cleanup_old_backups("_db_backups", keep_days=30)  # Keep 30 days
```

### Custom Notification System

Implement notifications in the `send_notification()` function:
```python
def send_notification(success, operation, error_details=None):
    # Add your email/Slack/webhook logic here
    if success:
        send_slack_message(f"✅ Database {operation} successful")
    else:
        send_email_alert(f"❌ Database {operation} failed: {error_details}")
```

### Additional SQL Filtering

Add custom problematic patterns in `filter_sql_backup()`:
```python
problematic_patterns = [
    r"^SET transaction_timeout\s*=",
    r"^SET your_custom_parameter\s*=",  # Add custom patterns
]
```

## Performance

### Typical Performance Metrics

- **Backup Speed**: ~30-50 MB database in 1-2 seconds
- **Total Process Time**: 3-5 seconds for complete backup/restore cycle
- **Memory Usage**: Minimal - processes files as streams
- **Disk Space**: Temporary 2x database size during operation

### Optimization Tips

1. **Local Network**: Run on same network as database servers for faster transfers
2. **SSD Storage**: Use SSD for backup file storage
3. **PostgreSQL Tuning**: Optimize PostgreSQL settings for faster dumps
4. **Parallel Processing**: For very large databases, consider parallel dump options

## Security Considerations

### Password Management
- ✅ **Use `.pgpass`**: Secure, PostgreSQL-standard authentication
- ✅ **File Permissions**: Ensure `.pgpass` has 600 permissions
- ❌ **Avoid Hard-coding**: Never put passwords in configuration files
- ❌ **Avoid Command Line**: Don't pass passwords as command arguments

### Network Security
- Use encrypted connections (SSL/TLS) for remote database access
- Consider VPN or SSH tunneling for additional security
- Restrict database access to specific IP addresses

### Backup Security
- Store backups in secure locations with appropriate access controls
- Consider encrypting backup files for sensitive data
- Implement backup file rotation and secure deletion

## Troubleshooting

### Debug Mode

Enable debug logging by modifying `logging.yaml`:
```yaml
root:
    level: DEBUG  # Change from INFO to DEBUG
```

### Connection Testing

Test individual components:
```bash
# Test remote connection
psql -h your.remote.server.com -U your_user -d your_db

# Test local connection
psql -h localhost -U postgres -d postgres

# Test pg_dump access
pg_dump --version
```

### Manual Operations

Perform operations manually for debugging:
```bash
# Manual backup
pg_dump -h your.remote.server.com -U your_user -d your_db --format=plain > manual_backup.sql

# Manual restore
psql -h localhost -U postgres -d your_local_db < manual_backup.sql
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/improvement`)
3. Make your changes with appropriate tests
4. Update documentation as needed
5. Submit a pull request

## License

This script is provided as-is for educational and practical use. Please ensure compliance with your organization's policies and PostgreSQL licensing terms.

## Support

For issues and questions:
1. Check the troubleshooting section above
2. Review log files for detailed error information
3. Verify PostgreSQL client tools and permissions
4. Test connections manually before running the script

---

**Last Updated**: June 2025
**Compatible with**: PostgreSQL 13+, Python 3.13+