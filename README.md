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

# Dump database schema to file _with_ IF NOT EXISTS in CREATE TABLE statements
./synch dump-schema --if-not-exists <clickhouse_url> <file> <database>

# <clickhouse_url> here looks like `"clickhouse://user:password@host:port"`

# Synchronize a table across clusters
./synch synctable <table_name>

# Replay query history between clusters for benchmarking
./synch replay <cluster> <from_clickhouse_url> <to_clickhouse_url> <start_date> <end_date>
```

## Configuration

The application is configured via environment variables. Check out `.env.sample` for example env vars that are required
