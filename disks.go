package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

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

			pollconn, err := connectUS()
			if err != nil {
				panic(err)
			}
			defer pollconn.Close()
			for {
				select {
				case <-done:
					return
				default:
					rows, err := pollconn.Query(
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
							partSize       uint64
							threadID       uint64
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

func drainDisk(ctx context.Context, conn driver.Conn, disk, toDisk string) error {
	fmt.Printf("Draining disk: %s\n", disk)
	query := "select database, table, disk_name, sum(bytes) b, formatReadableSize(sum(bytes)) size, count(1) parts " +
		"from system.parts where disk_name = {disk_name:String} group by database, table, disk_name order by disk_name desc;"
	rows, err := conn.Query(ctx, query, clickhouse.Named("disk_name", disk))
	if err != nil {
		log.Fatal(err)
		return err
	}
	for rows.Next() {
		var (
			database string
			table    string
			diskName string
			bytes    uint64
			size     string
			parts    uint64
		)
		if err := rows.Scan(
			&database,
			&table,
			&diskName,
			&bytes,
			&size,
			&parts,
		); err != nil {
			log.Fatal(err)
			return err
		}
		fmt.Printf("Moving Table: %s.%s, Disk: %s, Parts: %d, Size: %s To Disk: %s\n", database, table, diskName, parts, size, toDisk)
		err := moveTo(ctx, conn, database, table, disk, toDisk)
		if err != nil {
			log.Fatal(err)
			return err
		}
	}
	return nil
}
