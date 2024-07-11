package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/go-co-op/gocron"
	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
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
		Use:   "drain-disk",
		Short: "subcommand to move all parts of all tables from a disk to another disk <from_disk> <to_disk> as arguments",
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				fromDisk = args[0]
				toDisk   = args[1]
			)
			fmt.Printf("Moving parts to from disk %s to disk %s\n", fromDisk, toDisk)
			connUS, err := connectUS()
			if err != nil {
				panic(err)
			}
			ctx := context.Background()
			testConection(ctx, connUS)
			drainDisk(ctx, connUS, fromDisk, toDisk)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "dump-schema",
		Short: "dump schema to file <clickhouse_url> <file> <database> as arguments",
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				noKafkas      = false
				noMatViews    = false
				clickhouseUrl = &args[0]
				file          = &args[1]
				specifiedDB   = &args[2]
			)

			cmd.Flags().BoolVar(&noKafkas, "no-kafka", false, "Don't dump Kafka tables")
			cmd.Flags().BoolVar(&noMatViews, "no-mat-views", false, "Don't dump materialized views")

			conn, err := NewCHConn(clickhouseUrl)
			if err != nil {
				fmt.Printf("Error connecting to the database: %v\n", err)
				os.Exit(1)
			}
			defer conn.Close()

			opts := Options{
				DB:          conn,
				Path:        *file,
				SpecifiedDB: *specifiedDB,
				NoKafkas:    noKafkas,
				NoMatViews:  noMatViews,
			}

			err = Write(&opts)
			if err != nil {
				fmt.Printf("Error writing schema: %v\n", err)
				os.Exit(1)
			}

			if len(opts.Path) > 0 {
				fmt.Printf("Schema successfully saved to %s\n", *file)
			}
		},
	})

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

	cmd.AddCommand(&cobra.Command{
		Use:   "replay",
		Short: "Replay a portion of query history from one cluster onto another, for benchmarking. Arguments are cluster, start and stop dates.",
		Args:  cobra.MinimumNArgs(3),
		Run: func(cmd *cobra.Command, args []string) {

			var (
				cluster  = args[0]
				startStr = args[1]
				stopStr  = args[2]
				skipFile string
			)

			if len(args) > 3 {
				skipFile = args[3]
			}

			start, err := time.Parse("2006-01-02", startStr)
			if err != nil {
				log.Errorln(err)
				panic(err)
			}

			stop, err := time.Parse("2006-01-02", stopStr)
			if err != nil {
				log.Errorln(err)
				panic(err)
			}

			connEU, err := connectEU()
			if err != nil {
				log.Errorln(err)
				panic(err)
			}

			connCloud, err := connectCloud()
			if err != nil {
				log.Errorln(err)
				panic(err)
			}

			ctx := context.Background()
			testConection(ctx, connEU)
			testConection(ctx, connCloud)

			err = replayQueryHistory(ctx, connEU, connCloud, cluster, start, stop, skipFile)
			if err != nil {
				log.Errorln(err)
				panic(err)
			}
		},
	})

	// LETS GOOOOO
	cmd.Execute()
}
