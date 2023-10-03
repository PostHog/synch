package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-co-op/gocron"
	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	tables = []string{
		"person_static_cohort",
	}
)

func init() {
	// Load .env file. This populates the environment with key-value pairs from .env
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found")
	}
	viper.AutomaticEnv()
}

func main() {
	cmd := &cobra.Command{
		Use:   "ClickHouse tools",
		Short: "A simple ClickHouse admin application",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("This is a simple ClickHouse admin application")
		},
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "hello",
		Short: "subcommand test cobra",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Hi there", args[0])
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "movetohot",
		Short: "subcommand to move all parts of a table to hot",
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				database = args[0]
				table    = args[1]
			)
			fmt.Printf("Moving parts to hot for table:%s.%s\n", database, table)
			connUS, err := connectUS()
			if err != nil {
				panic(err)
			}
			ctx := context.Background()
			testConection(ctx, connUS)
			moveToHot(ctx, connUS, database, table)
		},
	})

	// Add additional commands to the command
	cmd.AddCommand(&cobra.Command{
		Use:   "synctable",
		Short: "subcommand to sync a table across clusters",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {

			connEU, err := connectEU()
			if err != nil {
				panic((err))
			}

			connCloud, err := connectCloud()
			if err != nil {
				panic((err))
			}

			ctx := context.Background()
			testConection(ctx, connEU)
			testConection(ctx, connCloud)

			s := gocron.NewScheduler(time.UTC)
			s.Every(1).Day().At("00:30").WaitForSchedule().Do(func() {
				updateTables(ctx, connEU, connCloud)
			})

			updateTables(ctx, connEU, connCloud)

			s.StartBlocking()
		},
	})
	cmd.Execute()
}

func testConection(ctx context.Context, conn driver.Conn) {
	rows, err := conn.Query(ctx, "SELECT name,toString(uuid) as uuid_str FROM system.tables LIMIT 5")
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var (
			name, uuid string
		)
		if err := rows.Scan(
			&name,
			&uuid,
		); err != nil {
			log.Fatal(err)
		}
		log.Printf("name: %s, uuid: %s",
			name, uuid)
	}
}

func moveToHot(ctx context.Context, conn driver.Conn, database, table string) error {
	rows, err := conn.Query(
		ctx,
		"select partition from system.parts where active and disk_name != 'hot' and database = {database:String} and table = {table:String} group partition;",
		clickhouse.Named("database", database),
		clickhouse.Named("table", table))
	if err != nil {
		log.Fatal(err)
		return err
	}

	for rows.Next() {
		var partition string
		if err := rows.Scan(
			&partition,
		); err != nil {
			log.Fatal(err)
			return err
		}
		fmt.Println("Moving partition:", partition)
		conn.Exec(
			ctx,
			"alter table {database:Identifier}.{table:Identifier} move partition {partition:String} to disk 'hot'",
			clickhouse.Named("database", database),
			clickhouse.Named("table", table),
			clickhouse.Named("partition", partition))
	}
	return nil
}

func updateTables(ctx context.Context, connEU, connCloud driver.Conn) {
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

func connectCloud() (driver.Conn, error) {
	addr := fmt.Sprintf("%s:%d", viper.GetString("CLICKHOUSE_CLOUD_HOSTNAME"), viper.GetInt("CLICKHOUSE_CLOUD_PORT"))
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{addr},
			Auth: clickhouse.Auth{
				Database: viper.GetString("CLICKHOUSE_CLOUD_DATABASE"),
				Username: viper.GetString("CLICKHOUSE_CLOUD_USERNAME"),
				Password: viper.GetString("CLICKHOUSE_CLOUD_PASSWORD"),
			},
			ClientInfo: clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "an-example-go-client", Version: "0.1"},
				},
			},

			Debugf: func(format string, v ...interface{}) {
				fmt.Printf(format, v)
			},
			TLS: &tls.Config{
				InsecureSkipVerify: true,
			},
		})
	)

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}
	return conn, nil
}

func connectEU() (driver.Conn, error) {
	addr := fmt.Sprintf("%s:%d", viper.GetString("CLICKHOUSE_EU_HOSTNAME"), viper.GetInt("CLICKHOUSE_EU_PORT"))
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{addr},
			Auth: clickhouse.Auth{
				Database: viper.GetString("CLICKHOUSE_EU_DATABASE"),
				Username: viper.GetString("CLICKHOUSE_EU_USERNAME"),
				Password: viper.GetString("CLICKHOUSE_EU_PASSWORD"),
			},
			ClientInfo: clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "an-example-go-client", Version: "0.1"},
				},
			},

			Debugf: func(format string, v ...interface{}) {
				fmt.Printf(format, v)
			},
			TLS: &tls.Config{
				InsecureSkipVerify: true,
			},
		})
	)

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}
	return conn, nil
}

func connectUS() (driver.Conn, error) {
	addr := fmt.Sprintf("%s:%d", viper.GetString("CLICKHOUSE_US_HOSTNAME"), viper.GetInt("CLICKHOUSE_US_PORT"))
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{addr},
			Auth: clickhouse.Auth{
				Database: viper.GetString("CLICKHOUSE_US_DATABASE"),
				Username: viper.GetString("CLICKHOUSE_US_USERNAME"),
				Password: viper.GetString("CLICKHOUSE_US_PASSWORD"),
			},
			ClientInfo: clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "an-example-go-client", Version: "0.1"},
				},
			},

			Debugf: func(format string, v ...interface{}) {
				fmt.Printf(format, v)
			},
			TLS: &tls.Config{
				InsecureSkipVerify: true,
			},
		})
	)

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}
	return conn, nil
}
