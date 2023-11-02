package main

import (
	"context"
	"fmt"
	"log"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func dumpTableSchema(ctx context.Context, conn driver.Conn, database, table string) {
	rows, err := conn.Query(
		ctx,
		"SHOW CREATE TABLE {database:Identifier}.{table:Identifier};",
		clickhouse.Named("database", database),
		clickhouse.Named("table", table))
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var (
			row string
		)
		if err := rows.Scan(
			&row,
		); err != nil {
			log.Fatal(err)
		}
		fmt.Println("\n/* CREATE TABLE STATEMENT FOR TABLE:", table, " */")
		fmt.Println(row)
	}
}

func dumpSchema(ctx context.Context, conn driver.Conn, database string) {
	rows, err := conn.Query(ctx, "select name from system.tables where database = {database:String} group by name order by name;", clickhouse.Named("database", database))
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var (
			tableName string
		)
		if err := rows.Scan(
			&tableName,
		); err != nil {
			log.Fatal(err)
		}
		dumpTableSchema(ctx, conn, database, tableName)
	}
}
