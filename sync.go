package main

import (
	"context"
	"fmt"
	"log"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/spf13/viper"
)

func updateTables(connEU, connCloud driver.Conn) {
	for _, table := range tables {
		if err := updateTable(context.Background(), connEU, connCloud, viper.GetString("CLICKHOUSE_EU_DATABASE"), table, false, true); err != nil {
			panic(err)
		}
	}
}

func updateTable(ctx context.Context, connEU, connCloud driver.Conn, database, table string, truncate, dryRun bool) error {
	if truncate {
		truncateStatement := createTruncateStatement(database, table)
		fmt.Printf("on CH Cloud Truncating table %s.%s\nTruncate statement:\n%s\n", database, table, truncateStatement)
		if !dryRun {
			if err := connCloud.Exec(ctx, truncateStatement); err != nil {
				return err
			}
		}
	}

	insertStatement := createInsertStatement(
		viper.GetString("CLICKHOUSE_CLOUD_HOSTNAME"),
		viper.GetInt("CLICKHOUSE_CLOUD_PORT"),
		database,
		table,
		viper.GetString("CLICKHOUSE_CLOUD_USERNAME"),
		viper.GetString("CLICKHOUSE_CLOUD_PASSWORD"))
	fmt.Printf("on EU CH inserting into table %s.%s\nInsert statement:\n%s\n", database, table, insertStatement)
	if !dryRun {
		if err := connEU.Exec(ctx, insertStatement); err != nil {
			return err
		}
	}

	selectStatement := createSelectStatement(database, table)
	fmt.Printf("on CH Cloud selecting from table %s.%s\nSelect statement:\n%s\n", database, table, selectStatement)
	rows, err := connCloud.Query(ctx, selectStatement)
	if err != nil {
		return err
	}
	for rows.Next() {
		var (
			tableRows uint64
		)
		if err := rows.Scan(
			&tableRows,
		); err != nil {
			log.Fatal(err)
		}
		log.Printf("Rows in table: %d", tableRows)
	}

	return nil
}

func createSelectStatement(database, table string) string {
	const SELECT_QUERY = `SELECT count(1) table_rows FROM %s.%s`

	return fmt.Sprintf(SELECT_QUERY, database, table)
}

func createTruncateStatement(database, table string) string {
	const TRUNCATE_QUERY = `TRUNCATE TABLE %s.%s settings max_table_size_to_drop = '9999999999999999999'`

	return fmt.Sprintf(TRUNCATE_QUERY, database, table)
}

func createInsertStatement(host string, port int, database, table, username, password string) string {
	const INSERT_QUERY = `INSERT INTO FUNCTION
remoteSecure('%s:%d', '%s.%s', '%s', '%s')
SELECT * from %s.%s.
settings max_execution_time = 999999999`

	return fmt.Sprintf(INSERT_QUERY, host, port, database, table, username, password, database, table)
}
