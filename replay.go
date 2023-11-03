package main

import (
	"context"
	"encoding/csv"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	log "github.com/sirupsen/logrus"
)

type Query struct {
	queryKind       string
	query           string
	queryStartTime  time.Time
	queryDurationMs uint64
}

type QueryResult struct {
	queryKind          string
	query              string
	originalStartTime  time.Time
	originalDurationMs uint64
	replayStartTime    time.Time
	replayDurationMs   uint64
	deltaMs            uint64
	queryErrored       bool
}

func worker(id int, queries <-chan Query, results chan<- QueryResult) {
	log.Info("Starting worker: ", id)
	// TODO: fix this to be more dynamic and not just connect to cloud
	conn, err := connectCloud()
	ctx := context.Background()
	if err != nil {
		panic(err)
	}
	for q := range queries {
		start := time.Now()
		queryErrored := false
		log.Println("worker", id, "started  job", q)
		err := conn.Exec(ctx, q.query)
		if err != nil {
			log.Warn(err)
		}
		end := time.Now()
		queryResult := QueryResult{
			queryKind:          q.queryKind,
			query:              q.query,
			originalStartTime:  q.queryStartTime,
			originalDurationMs: q.queryDurationMs,
			replayStartTime:    start,
			replayDurationMs:   uint64(end.Sub(start).Milliseconds()),
			deltaMs:            q.queryDurationMs - uint64(end.Sub(start).Milliseconds()),
			queryErrored:       queryErrored,
		}
		results <- queryResult
	}
}

func csvWriter(results <-chan QueryResult, wg *sync.WaitGroup) {
	log.Info("Starting CSV writer")

	i := 0

	// Create a new CSV file
	file, err := os.Create("output.csv")
	if err != nil {
		log.Fatalf("failed creating file: %s", err)
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write all the records
	for r := range results {
		i++
		if err := writer.Write(
			[]string{
				strconv.Itoa(i),
				r.query,
				r.originalStartTime.Format(time.RFC3339),
				r.replayStartTime.Format(time.RFC3339),
				strconv.FormatUint(r.originalDurationMs, 10),
				strconv.FormatUint(r.replayDurationMs, 10),
				strconv.FormatUint(r.deltaMs, 10),
			}); err != nil {
			log.Fatalln("error writing record to csv:", err)
			wg.Done()
		}
		writer.Flush()
		// Check if there are any errors during write
		if err := writer.Error(); err != nil {
			log.Fatal(err)
		}
		wg.Done()
	}

}

func replayQueryHistory(ctx context.Context, fromConn, toConn driver.Conn, cluster string, start, stop time.Time) error {
	log.Info("Starting workers")
	numWorkers := 1000

	queries := make(chan Query)
	results := make(chan QueryResult)
	var wg sync.WaitGroup
	for w := 1; w <= numWorkers; w++ {
		go worker(w, queries, results)
	}

	// start the csv writer
	go csvWriter(results, &wg)

	log.Infof("Replaying query history from %s to %s", start.Format("2006-01-02"), stop.Format("2006-01-02"))
	rows, err := fromConn.Query(ctx, "select query_kind, query, query_start_time, query_duration_ms "+
		"from clusterAllReplicas({cluster:String}, system.query_log) "+
		"where type = 2 and is_initial_query = 1 and query_kind = 'Select' "+
		"and query_start_time >= {start:String} and query_start_time <= {stop:String} "+
		"group by query, query_start_time, query_duration_ms, query_kind "+
		"order by query_start_time desc "+
		"limit 10000000",
		clickhouse.Named("cluster", cluster),
		clickhouse.Named("start", start.Format("2006-01-02")),
		clickhouse.Named("stop", stop.Format("2006-01-02")))
	if err != nil {
		log.Fatal(err)
	}
	var lastQueryTS time.Time
	lastRunTS := time.Now()
	for rows.Next() {
		var (
			queryKind       string
			query           string
			queryStartTime  time.Time
			queryDurationMs uint64
		)
		if err := rows.Scan(
			&queryKind,
			&query,
			&queryStartTime,
			&queryDurationMs,
		); err != nil {
			log.Fatal(err)
		}
		queryRow := Query{
			queryKind:       queryKind,
			query:           query,
			queryStartTime:  queryStartTime,
			queryDurationMs: queryDurationMs,
		}
		if lastQueryTS.IsZero() {
			lastQueryTS = queryStartTime
		}
		for {
			timeSinceLastRun := time.Since(lastRunTS)
			timeToWait := queryStartTime.Sub(lastQueryTS)
			if timeSinceLastRun > timeToWait {
				wg.Add(1)
				queries <- queryRow
				lastQueryTS = queryStartTime
				lastRunTS = time.Now()
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	close(queries)
	wg.Wait()
	return nil
}
