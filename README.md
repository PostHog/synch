# synch - ClickHouse Administration Tool

`synch` is a CLI tool designed to facilitate the administration and synchronization of ClickHouse databases. It provides commands to move table parts between disks, drain disks, dump schemas, synchronize tables across clusters, and replay query history for benchmarking purposes.

## Features

- Move table parts between disks within the same database.
- Move parts from all tables from one disk to another.
- Dump database schemas to a file.
- Synchronize a table across different clusters.
- Replay a portion of the query history for benchmarking.

## Installation

```bash
# Clone the repository
git clone https://github.com/posthog/synch.git

# Navigate to the synch directory
cd synch

# Build the project for AMD64
GOARCH=amd64 GOOS=linux go build -o synch

# ALTERNATIVELY Build the project for ARM64
GOARCH=arm64 GOOS=linux go build -o synch


# SCP it to wherever you are going to use it
scp synch ch.instance.dev:

```

## Usage

Before using the tool, ensure that all environmental variables required for database connections are properly set or provided in a .env file.

Here's a quick rundown of the commands available:

```bash
# Basic usage
./synch

# Move table parts from one disk to another
./synch moveto <from_disk> <to_disk> <database> <table>

# Drain all parts from one disk to another
./synch drain-disk <from_disk> <to_disk>

# Dump database schema to file
./synch dump-schema <clickhouse_url> <file> <database>

# Dump database schema to file _without_ kafka or materialized view tables
./synch dump-schema --no-kafkas --no-mat-views <clickhouse_url> <file> <database>

# Dump database schema to file _without_ Distributed tables
./synch dump-schema --no-distributed <clickhouse_url> <file> <database>

# Dump database schema to file _with_ IF NOT EXISTS in CREATE TABLE statements
./synch dump-schema --if-not-exists <clickhouse_url> <file> <database>

# Dump database schema to file _with_ secret values intact
# This is intended for very specific situations and should always be used with care.
# Do NOT use this if you are not sure, and do NOT leave the resulting dump file behind when you are done!
# See "--show-secrets prerequisites" below for required cluster-side configuration.
./synch dump-schema --show-secrets <clickhouse_url> <file> <database>

# <clickhouse_url> here looks like `"clickhouse://user:password@host:port"`

# Synchronize a table across clusters
./synch synctable <table_name>

# Replay query history between clusters for benchmarking
./synch replay <cluster> <from_clickhouse_url> <to_clickhouse_url> <start_date> <end_date>
```

### `--show-secrets` prerequisites

For `--show-secrets` to actually replace `[HIDDEN]` with real values, the **ClickHouse cluster** must be configured to permit secret display. All of the following are required — if any is missing, the dump will still contain `[HIDDEN]` and the flag becomes a silent no-op:

1. **Server config** — add the following to `config.xml`, or as an overlay file in `config.d/` (e.g. `/etc/clickhouse-server/config.d/display-secrets.xml`):

    ```xml
    <clickhouse>
      <display_secrets_in_show_and_select>1</display_secrets_in_show_and_select>
    </clickhouse>
    ```

    This setting is **not** changeable without restart. After updating the config, restart the ClickHouse server for it to take effect. You can confirm it is enabled with:

    ```sql
    SELECT value FROM system.server_settings WHERE name = 'display_secrets_in_show_and_select';
    ```

2. **User privilege** — the connecting user must hold the `displaySecretsInShowAndSelect` privilege.

    Verify the connecting user holds it with:

    ```sql
    CHECK GRANT displaySecretsInShowAndSelect ON *.*;
    ```

    A return value of `1` means the privilege is held. If it returns `0`, grant it (the exact statement may vary by your access-control setup, but typically: `GRANT displaySecretsInShowAndSelect ON *.* TO <user>`).

    If they do not, grant it with the following and re-run the check:

    ```sql
    GRANT displaySecretsInShowAndSelect ON *.* TO <user>;
    ```

**Operational warning**: dumps produced with `--show-secrets` contain plaintext credentials (passwords inside `ENGINE = ...` arguments and dictionary `SOURCE(...)` clauses). Do not commit the file, do not share it through insecure channels, and delete it as soon as it has served its purpose.

## Configuration

The application is configured via environment variables. Check out `.env.sample` for example env vars that are required
