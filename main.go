package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

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
		Use:   "moveto",
		Short: "subcommand to move all parts of a table from a disk to another disk <from_disk> <to_disk> <database> <table> as arguments",
		Args:  cobra.MinimumNArgs(3),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				fromDisk = args[0]
				toDisk   = args[1]
				database = args[2]
				table    = args[3]
			)
			fmt.Printf("Moving parts to from disk %s to disk %s for table: %s.%s\n", fromDisk, toDisk, database, table)
			connUS, err := connectUS()
			if err != nil {
				panic(err)
			}
			ctx := context.Background()
			testConection(ctx, connUS)
			moveTo(ctx, connUS, database, table, fromDisk, toDisk)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "dump-schema",
		Short: "dump schema to file just include <database> as argument",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				database = args[0]
			)
			connUS, err := connectUS()
			if err != nil {
				panic(err)
			}
			ctx := context.Background()
			testConection(ctx, connUS)
			dumpSchema(ctx, connUS, database)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "dump-schema",
		Short: "dump schema to file just include <database> as argument",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				database = args[0]
			)
			connUS, err := connectUS()
			if err != nil {
				panic(err)
			}
			ctx := context.Background()
			testConection(ctx, connUS)
			dumpSchema(ctx, connUS, database)
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
	rows, err := conn.Query(ctx, "SELECT hostname(), version()")
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var (
			hostname, version string
		)
		if err := rows.Scan(
			&hostname,
			&version,
		); err != nil {
			log.Fatal(err)
		}
		log.Printf("Connected to %s with server version of %s", hostname, version)
	}
}

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

func moveTo(ctx context.Context, conn driver.Conn, database, table, fromDisk, toDisk string) error {
	fmt.Printf("Moving parts for table: %s.%s from disk %s to disk %s\n", database, table, fromDisk, toDisk)
	rows, err := conn.Query(
		ctx,
		"select name from system.parts where active and disk_name = {fromDisk:String} and database = {database:String} and table = {table:String} group by name;",
		clickhouse.Named("fromDisk", fromDisk),
		clickhouse.Named("database", database),
		clickhouse.Named("table", table))
	if err != nil {
		log.Fatal(err)
		return err
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(
			&name,
		); err != nil {
			log.Fatal(err)
			return err
		}
		fmt.Printf("Moving part: %s for table %s.%s to disk %s\n", name, database, table, toDisk)
		fmtQuery := fmt.Sprintf("ALTER TABLE {database:Identifier}.{table:Identifier} move part '%s' to disk '%s'", name, toDisk)

		done := make(chan bool)
		go func() {
			for {
				select {
				case <-done:
					return
				default:
					rows, err := conn.Query(
						ctx,
						"select database, table, elapsed, target_disk_name, target_disk_path, part_name, part_size, thread_id from system.moves",
					)
					if err != nil {
						log.Fatal(err)
					}
					for rows.Next() {
						var (
							database       string
							table          string
							elapsed        float64
							targetDiskName string
							targetDiskPath string
							partName       string
							partSize       int64
							threadID       int
						)
						if err := rows.Scan(
							&database,
							&table,
							&elapsed,
							&targetDiskName,
							&targetDiskPath,
							&partName,
							&partSize,
							&threadID,
						); err != nil {
							log.Fatal(err)
						}
						fmt.Printf("Moving part %s for table %s.%s to disk %s (%s) [elapsed: %f, size: %d, thread: %d]\n", partName, database, table, targetDiskName, targetDiskPath, elapsed, partSize, threadID)
					}
					time.Sleep(2 * time.Second)
				}
			}
		}()
		err = conn.Exec(
			ctx,
			fmtQuery,
			clickhouse.Named("database", database),
			clickhouse.Named("table", table),
			clickhouse.Named("part_name", name))
		if err != nil {
			log.Fatal(err)
			done <- true
			return err
		}
		done <- true
	}
	return nil
}

func drainDisk(ctx context.Context, conn driver.Conn, disk string) error {
	fmt.Printf("Draining disk: %s\n", disk)

	// Get all tables with active parts on disk to drain
	dbTables, err := conn.Query(
		ctx,
		"select database, table from system.parts where active and disk_name = {disk:String} group by database, table;",
		clickhouse.Named("disk", disk))
	if err != nil {
		log.Fatal(err)
		return err
	}

	for dbTables.Next() {
		var (
			database, table string
		)
		if err := dbTables.Scan(
			&database,
			&table,
		); err != nil {
			log.Fatal(err)
			return err
		}

		// get the storage policy for the table
		rows, err := conn.Query(
			ctx,
			"select storage_policy from system.tables where database = {database:String} and table = {table:String} group by storage_policy;",
			clickhouse.Named("database", database),
			clickhouse.Named("table", table))
		if err != nil {
			log.Fatal(err)
			return err
		}
		var storagePolicy string
		for rows.Next() {
			if err := rows.Scan(
				&storagePolicy,
			); err != nil {
				log.Fatal(err)
				return err
			}
		}

		// get disks in storage policy
		rows, err = conn.Query(
			ctx,
			"select disks from system.storage_policies where name = {storagePolicy:String} group by disks;",
			clickhouse.Named("storagePolicy", storagePolicy))
		if err != nil {
			log.Fatal(err)
			return err
		}
		var disks []string
		for rows.Next() {
			if err := rows.Scan(
				&disks,
			); err != nil {
				log.Fatal(err)
				return err
			}
		}

		// Get all parts for table on disk to drain
		parts, err := conn.Query(
			ctx,
			"select name from system.parts where active and disk_name = {disk:String} and database = {database:String} and table = {table:String} group by name;",
			clickhouse.Named("disk", disk),
			clickhouse.Named("database", database),
			clickhouse.Named("table", table))
		if err != nil {
			log.Fatal(err)
			return err
		}
		for parts.Next() {
			var name string
			if err := parts.Scan(
				&name,
			); err != nil {
				log.Fatal(err)
				return err
			}

			// chose a disk to move the table to based on available disk
			toDisk, err := conn.Query(
				ctx,
				"select name from system.disks where name != {disk:String} and name in {disks:Array(String)} order by free_space desc limit 1;",
				clickhouse.Named("disk", disk),
				clickhouse.Named("disks", disks))
			if err != nil {
				log.Fatal(err)
				return err
			}
			var toDiskName string
			for toDisk.Next() {
				if err := toDisk.Scan(
					&toDiskName,
				); err != nil {
					log.Fatal(err)
					return err
				}
			}

			// move part to new disk
			fmt.Printf("Moving part: %s for table %s.%s to disk %s\n", name, database, table, toDiskName)
			fmtQuery := fmt.Sprintf("alter table {database:Identifier}.{table:Identifier} move part '%s' to disk '%s'", name, toDiskName)
			fmt.Printf("Query: %s\n", fmtQuery)
			err = conn.Exec(
				ctx,
				fmtQuery,
				clickhouse.Named("database", database),
				clickhouse.Named("table", table),
				clickhouse.Named("part_name", name),
				clickhouse.Named("toDiskName", toDiskName))
			if err != nil {
				log.Fatal(err)
				return err
			}
		}
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
			ReadTimeout: 300 * time.Minute,
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
