package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/spf13/viper"
)

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
			Settings: clickhouse.Settings{
				"move_all_conditions_to_prewhere":                      1,
				"enable_multiple_prewhere_read_steps":                  0,
				"use_hedged_requests":                                  0,
				"allow_experimental_parallel_reading_from_replicas":    1,
				"max_parallel_replicas":                                6,
				"parallel_replicas_single_task_marks_count_multiplier": 0.125,
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
