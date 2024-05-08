package backups

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/spf13/viper"
)

func loadConfigs() {
	viper.SetConfigName("backups")
	viper.AddConfigPath("configs")
	viper.SetConfigType("yaml")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %w", err))
	}
}

func connectClickhouse(ctx context.Context, host, port, database, username, password string) clickhouse.Conn {

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", host, port)},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "sync-client", Version: "0.6.9"},
			},
		},
		Debugf: func(format string, v ...interface{}) {
			fmt.Printf(format, v)
		},
		Settings: clickhouse.Settings{},
		TLS: &tls.Config{
			InsecureSkipVerify: true,
		},
	})
	if err != nil {
		log.Panicf("Cannot connect: %v", err)
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			log.Panicf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
	}
	return conn
}

func listTables() []string {
	// This could be helpful for backing up all tables that are important (to us)
	ctx := context.Background()
	var (
		host     = viper.GetString("backup.cluster.host")
		port     = viper.GetString("backup.cluster.port")
		database = viper.GetString("backup.cluster.database")
		username = viper.GetString("backup.cluster.username")
		password = viper.GetString("backup.cluster.password")
	)

	log.Printf("Connecting to Clickhouse at %s:%s\n", host, port)
	conn := connectClickhouse(ctx, host, port, database, username, password)
	defer conn.Close()

	rows, err := conn.Query(
		ctx,
		`
		SELECT name, if(name LIKE 'sharded%', true, false) as sharded
		FROM system.tables
		WHERE database = {database:String} 
		AND engine LIKE '%Replicated%'	
		`,
		clickhouse.Named("database", database),
	)
	if err != nil {
		log.Fatal(err)
	}

	tables := make([]string, 0)
	for rows.Next() {
		var (
			table   string
			sharded bool
		)
		if err := rows.Scan(&table, &sharded); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Table: %s, Sharded: %t\n", table, sharded)
		tables = append(tables, table)
	}
	return tables
}
