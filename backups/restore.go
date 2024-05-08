package backups

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/spf13/viper"
)

func restoreTable(bucket, table, backup_ts string, shards []string, restoreSharded bool) {
	loadConfigs()

	var (
		host     = viper.GetString("restore.cluster.host")
		port     = viper.GetString("restore.cluster.port")
		database = viper.GetString("restore.cluster.database")
		username = viper.GetString("restore.cluster.username")
		password = viper.GetString("restore.cluster.password")
		// Access information for getting to the backups
		s3AccessKey = viper.GetString("backup.s3.access_key_id")
		s3SecretKey = viper.GetString("backup.s3.secret_access_key")

		// allowNonEmptyTables    = viper.GetBool("restore.cluster.allow_non_empty_tables")
		// allowDifferentTableDef = viper.GetBool("restore.cluster.allow_different_table_def")

		restoreQuery string
	)

	log.Printf("Connecting to Clickhouse at %s:%s\n", host, port)

	ctx := context.Background()
	conn := connectClickhouse(ctx, host, port, database, username, password)
	defer conn.Close()

	if restoreSharded {
		panic("NOT IMPLEMENTED")
	} else {
		restoreTable := strings.Replace(table, "sharded_", "", 1)

		for _, shard := range shards {
			log.Printf("Restoring table %s shard %s from backup %s\n", table, shard, backup_ts)
			// Restore the table

			restoreQuery = fmt.Sprint(
				"RESTORE TABLE ", database, ".", table, " AS ", database, ".", restoreTable,
				" FROM S3('https://", bucket, ".s3.amazonaws.com/", table, "/", backup_ts, "/", shard, "', '", s3AccessKey, "','", s3SecretKey, "')",
				" SETTINGS allow_non_empty_tables = true, allow_different_table_def = true",
				" ASYNC ")

			log.Println("Restore table source:", table, "and destination", restoreTable)
			log.Println("Restore query:", restoreQuery)

			err := conn.Exec(
				ctx,
				restoreQuery,
			)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

}

func RestoreRecentBackup(table string) {
	// Restore a table from a backup
	restoreSharded := viper.GetBool("restore.cluster.sharded")
	backup := recentBackup(table)
	fmt.Printf("Restoring table %s from backup %s\n", backup.Table, backup.Backup_ts)
	restoreTable(backup.Bucket, backup.Table, backup.Backup_ts, backup.Shards, restoreSharded)
}

func RestoreRecentBackups() {
	// Restore a table from a backup
	restoreSharded := viper.GetBool("restore.cluster.sharded")
	backups := recentBackups()
	for _, backup := range backups {
		fmt.Printf("Restoring table %s from backup %s\n", backup.Table, backup.Backup_ts)
		restoreTable(backup.Bucket, backup.Table, backup.Backup_ts, backup.Shards, restoreSharded)
	}
}
