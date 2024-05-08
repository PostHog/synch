package main

import (
	"context"
	"fmt"
	"posthog/synch/backups"
	"time"

	"github.com/go-co-op/gocron"
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
	viper.AutomaticEnv()
}

func main() {
	cmd := &cobra.Command{
		Use:   "ClickHouse",
		Short: "A simple ClickHouse admin CLI",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("This is a simple ClickHouse admin application")
		},
	}

	tableCmd := &cobra.Command{
		Use:   "tables",
		Short: "simple table management commands (usually across clusters)",
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("This is the tables management command")
		},
	}

	tableCmd.AddCommand(&cobra.Command{
		Use:   "compare",
		Short: "subcommand to compare two tables from different clusters. Arguments are tableA and tableB",
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var (
				tableA = args[0]
				tableB = args[1]
			)

			fmt.Printf("Comparing table %s with table %s\n", tableA, tableB)
			backups.CompareTableDef(tableA, tableB)
		},
	})

	cmd.AddCommand(tableCmd)

	backupCmd := &cobra.Command{
		Use:   "backups",
		Short: "Backup and restore commands",
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("This is the backups management command")
		},
	}

	backupCmd.AddCommand(&cobra.Command{
		Use:   "list_recent",
		Short: "subcommand to list all backups",
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 1 {
				table := args[0]
				backups.ListRecentBackup(table)
				return
			}

			backups.ListRecentBackups()
		},
	})

	backupCmd.AddCommand(&cobra.Command{
		Use:   "restore_recent",
		Short: "subcommand to restore all backups",
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 1 {
				table := args[0]
				backups.RestoreRecentBackup(table)
				return
			}
			backups.RestoreRecentBackups()
		},
	})

	cmd.AddCommand(backupCmd)

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

	cmd.Execute()
}
