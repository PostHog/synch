package backups

import (
	"context"
	"fmt"
	"log"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/sergi/go-diff/diffmatchpatch"
	"github.com/spf13/viper"
)

func CompareTableDef(tableA, tableB string) {
	loadConfigs()

	var (
		hostA     = viper.GetString("backup.cluster.host")
		portA     = viper.GetString("backup.cluster.port")
		databaseA = viper.GetString("backup.cluster.database")
		usernameA = viper.GetString("backup.cluster.username")
		passwordA = viper.GetString("backup.cluster.password")

		hostB     = viper.GetString("restore.cluster.host")
		portB     = viper.GetString("restore.cluster.port")
		databaseB = viper.GetString("restore.cluster.database")
		usernameB = viper.GetString("restore.cluster.username")
		passwordB = viper.GetString("restore.cluster.password")
	)

	log.Printf("Connecting to Clickhouse at %s:%s\n", hostA, portA)
	log.Printf("Connecting to Clickhouse at %s:%s\n", hostB, portB)

	// Connect to Clickhouse A
	ctx := context.Background()
	connA := connectClickhouse(ctx, hostA, portA, databaseA, usernameA, passwordA)
	defer connA.Close()

	// Connect to Clickhouse B
	connB := connectClickhouse(ctx, hostB, portB, databaseB, usernameB, passwordB)
	defer connB.Close()

	// Get the table definition for tableA
	tableADef := getTableDef(ctx, connA, databaseA, tableA)
	tableBDef := getTableDef(ctx, connB, databaseB, tableB)

	if tableADef == tableBDef {
		log.Printf("Table definitions for %s.%s are the same\n", databaseA, tableA)
	} else {
		log.Printf("Table definitions for %s.%s are different\n", databaseA, tableA)
		log.Printf("Diff between tables:")

		dmp := diffmatchpatch.New()
		diffs := dmp.DiffMain(tableADef, tableBDef, false)
		fmt.Println(dmp.DiffPrettyText(diffs))
	}
}

func getTableDef(ctx context.Context, conn clickhouse.Conn, database, table string) string {
	rows, err := conn.Query(
		ctx,
		`
		SHOW CREATE TABLE {database:Identifier}.{table:Identifier}
		`,
		clickhouse.Named("database", database),
		clickhouse.Named("table", table),
	)
	if err != nil {
		log.Fatal(err)
	}

	var createTable string
	for rows.Next() {
		if err := rows.Scan(&createTable); err != nil {
			log.Fatal(err)
		}
	}

	return createTable
}
