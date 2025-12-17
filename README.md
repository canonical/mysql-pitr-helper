# MySQL Point-In-Time-Recovery Helper

This tool uploads [MySQL binlogs](https://dev.mysql.com/doc/refman/8.4/en/binary-log.html)
to S3-compatible or Azure blob storage and provides point-in-time recovery capabilities
for MySQL/PXC clusters.
It makes use of Percona's [`binlog_utils_udf`](https://github.com/percona/percona-server/tree/8.4/components/binlog_utils_udf)
to handle GTID sets and binary log files.

It is similar to PostgreSQL's [pgBackRest](https://pgbackrest.org/) and other tools.

## Features

- **Binlog Collection**: Continuously uploads binary logs to cloud storage (S3 or Azure Blob)
- **Point-in-Time Recovery**: Restore database to a specific timestamp, transaction, or latest state
- **GTID-Based**: Uses [global transaction identifiers](https://dev.mysql.com/doc/refman/8.4/en/replication-gtids-concepts.html) for reliable, consistent recovery
- **PXC Cluster Support**: Works with Percona XtraDB Cluster environments
- **Multiple Recovery Modes**: Latest, date-based, transaction-based, or skip specific transactions

## Usage

### Collector

```bash
mysql-pitr-helper collect [config.yaml]
```

You can configure the collector using environment variables or a YAML file.

YAML example:

```yaml
hosts: ["10.0.0.1", "10.0.0.2"]
user: backup_user
pass: secret
storage_type: s3
collect_span_sec: 60
s3:
  endpoint: s3.amazonaws.com
  access_key_id: AKIA...
  secret_access_key: secret
  bucket_url: my-bucket/mysql-binlogs
  default_region: us-east-1
```

**Global configurations:**
- `HOSTS` - MySQL cluster nodes (primary nodes only)
- `USER`, `PASS` - MySQL credentials
- `STORAGE_TYPE` - `s3` or `azure`
- `COLLECT_SPAN_SEC` - Collection interval (default: 60)
- `VERIFY_TLS` - TLS verification (default: true)

**Amazon S3 backend:**
- `ENDPOINT` - S3 endpoint (default: s3.amazonaws.com)
- `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY` - AWS credentials
- `S3_BUCKET_URL` - Bucket path (e.g., `my-bucket/backups`)
- `DEFAULT_REGION` - AWS region

**Azure Blob Storage backend:**
- `AZURE_ENDPOINT`
- `AZURE_CONTAINER_PATH`
- `AZURE_STORAGE_ACCOUNT`
- `AZURE_ACCESS_KEY`

For further information, see [`collector.go`](./collector/collector.go).

### Recoverer

```bash
mysql-pitr-helper recover
```

You can configure the recoverer using environment variables.

**Global configurations:**
- `HOST` - MySQL host to recover to
- `USER`, `PASS` - MySQL credentials
- `PITR_RECOVERY_TYPE` - Recovery mode: `latest`, `date`, `transaction`, `skip`
- `PITR_DATE` - Target timestamp (for `date` mode): `2006-01-02 15:04:05`
- `PITR_GTID` - GTID to restore/skip (for `transaction`/`skip` modes)
- `STORAGE_TYPE` - `s3` or `azure`

Storage credentials work similar to collector.

## How It Works

**Collection:**
1. Identifies healthy cluster nodes and selects the one with oldest binlogs
2. Periodically fetches binary logs using `mysqlbinlog --raw`
3. Uploads binlogs with GTID set metadata to cloud storage
4. Tracks last uploaded GTID set to avoid duplicates

**Recovery:**
1. Downloads binlogs from cloud storage
2. Filters binlogs based on current database GTID state
3. Applies binlogs using `mysqlbinlog | mysql` pipeline
4. Stops at specified recovery point (timestamp, GTID, or latest)

## Limitations

- No split-brain protection
