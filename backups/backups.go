package backups

import (
	"context"
	"fmt"
	"log"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/spf13/viper"
)

type Backup struct {
	Bucket    string   `ch:"bucket"`
	Table     string   `ch:"table"`
	Backup_ts string   `ch:"backup_ts"`
	Shards    []string `ch:"shards"`
	Status    []string `ch:"status"`
	Ids       []string `ch:"ids"`
}

func ListRecentBackup(table string) {
	backup := recentBackup(table)
	fmt.Println("Most recent successful backup:")
	fmt.Printf("Table:\t\t%s\n", backup.Table)
	fmt.Printf("Bucket:\t\t%s\n", backup.Bucket)
	fmt.Printf("Backup TS:\t%s\n", backup.Backup_ts)
	fmt.Printf("Shards:\t\t%v\n", backup.Shards)
	fmt.Printf("Status:\t\t%v\n", backup.Status)
	fmt.Printf("Ids:\t\t%v\n", backup.Ids)

	fmt.Println()
}

func recentBackup(table string) Backup {
	backups := recentBackups()
	for _, backup := range backups {
		if backup.Table == table {
			return backup
		}
	}
	return Backup{}
}

func ListRecentBackups() {
	backups := recentBackups()
	fmt.Println("Most recent successful backups:")
	for _, backup := range backups {
		fmt.Printf("Table:\t\t%s\n", backup.Table)
		fmt.Printf("Bucket:\t\t%s\n", backup.Bucket)
		fmt.Printf("Backup TS:\t%s\n", backup.Backup_ts)
		fmt.Printf("Shards:\t\t%v\n", backup.Shards)
		fmt.Printf("Status:\t\t%v\n", backup.Status)
		fmt.Printf("Ids:\t\t%v\n", backup.Ids)

		fmt.Println()
	}
}

func recentBackups() []Backup {
	loadConfigs()

	var (
		host     = viper.GetString("backup.cluster.host")
		port     = viper.GetString("backup.cluster.port")
		cluster  = viper.GetString("backup.cluster.cluster")
		database = viper.GetString("backup.cluster.database")
		username = viper.GetString("backup.cluster.username")
		password = viper.GetString("backup.cluster.password")
	)

	log.Printf("Connecting to Clickhouse at %s:%s\n", host, port)
	log.Printf("Listing backups for cluster %s\n", cluster)

	ctx := context.Background()
	conn := connectClickhouse(ctx, host, port, database, username, password)
	defer conn.Close()

	var backups []Backup

	// List all backups
	err := conn.Select(
		ctx,
		&backups,
		`
		WITH base as (
			SELECT
						id,
						name,
						status,
						start_time,
						end_time,
						total_size,
						arrayElement(arrayElement(extractAllGroupsVertical(name, '^S3\\(\'https:\/\/([^\/]+)\.s3\.amazonaws\.com\/([^\/]+)\/([^\/]+)\/(\d)\'.*$'), 1), 1) AS bucket,
						arrayElement(arrayElement(extractAllGroupsVertical(name, '^S3\\(\'https:\/\/([^\/]+)\.s3\.amazonaws\.com\/([^\/]+)\/([^\/]+)\/(\d)\'.*$'), 1), 2) AS table,
						arrayElement(arrayElement(extractAllGroupsVertical(name, '^S3\\(\'https:\/\/([^\/]+)\.s3\.amazonaws\.com\/([^\/]+)\/([^\/]+)\/(\d)\'.*$'), 1), 3) AS backup_ts,
						arrayElement(arrayElement(extractAllGroupsVertical(name, '^S3\\(\'https:\/\/([^\/]+)\.s3\.amazonaws\.com\/([^\/]+)\/([^\/]+)\/(\d)\'.*$'), 1), 4) AS shard
					FROM clusterAllReplicas('posthog', system.backups)
					ORDER BY end_time DESC, shard
			),
		ts_oriented as (
			SELECT bucket, table, backup_ts, arraySort(groupArray(shard)) shards, groupArray(status) status, groupArray(id) ids, row_number() OVER (PARTITION BY table ORDER BY backup_ts DESC) rn
			FROM base
			GROUP BY bucket, table, backup_ts
			),
		recent_backups as (
			SELECT bucket, table, backup_ts, shards, status, ids, rn FROM ts_oriented
			WHERE rn <= 2
			ORDER BY table DESC, rn
			),
		recent_successful_backups as (
			SELECT bucket, table, backup_ts, shards, status, ids, row_number() OVER (PARTITION BY table ORDER BY backup_ts DESC) rn FROM recent_backups AS rb
			WHERE has(status, 1) -- 1 is BACKUP_CREATED, 2 is BACKUP_FAILED, 3 is RESTORING, 4 is RESTORED, 5 is RESTORE_FAILED
			),
		recent_failed_backups as (
			SELECT bucket, table, backup_ts, shards, status, ids, row_number() OVER (PARTITION BY table ORDER BY backup_ts DESC) rn FROM recent_backups AS rb
			WHERE has(status, 2) -- 1 is BACKUP_CREATED, 2 is BACKUP_FAILED, 3 is RESTORING, 4 is RESTORED, 5 is RESTORE_FAILED
			)
			
			SELECT bucket, table, backup_ts, shards, status, ids
			FROM recent_successful_backups where rn = 1 -- Last successful backup
		`,
		clickhouse.Named("cluster", cluster))
	if err != nil {
		log.Fatal(err)
	}

	return backups
}
