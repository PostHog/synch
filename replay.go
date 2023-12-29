package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"strings"
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
	deltaMs            int64
	queryErrored       bool
	errorStr           string
}

func loadSkipQueries(file string) ([]string, error) {
	var skipQueries []string
	f, err := os.Open(file)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		// Split on semicolons instead of newlines
		for i := 0; i < len(data); i++ {
			if data[i] == ';' {
				return i + 1, data[:i], nil
			}
		}
		if atEOF && len(data) > 0 {
			return len(data), data, nil
		}
		return 0, nil, nil
	})

	for scanner.Scan() {
		query := scanner.Text()
		query = strings.TrimSpace(query)
		query = strings.ReplaceAll(query, "\n", " ")
		skipQueries = append(skipQueries, query)
	}
	if err := scanner.Err(); err != nil {
		log.Error(err)
		return nil, err
	}
	return skipQueries, nil
}

func buildQueryLogQuery(skipQueries []string) (string, error) {
	skipQueriesStr := []string{}
	for _, q := range skipQueries {
		q = strings.TrimSpace(q)
		q = strings.ReplaceAll(q, "\"", "\\\"")
		q = strings.ReplaceAll(q, "'", "\\'")
		q = strings.ReplaceAll(q, ";", "")
		skipQueriesStr = append(skipQueriesStr, "query like '%"+q+"%'")
	}

	var skipQueriesPredicate string
	if len(skipQueriesStr) == 0 {
		skipQueriesPredicate = ""
	} else {
		skipQueriesPredicate = `and (` + strings.Join(skipQueriesStr, " or ") + `)`
	}

	query := `
select normalized_query_hash from system.query_log
where type = 2 and is_initial_query = 1 and query_kind = 'Select'
and query_start_time >= {start:String} and query_start_time <= {stop:String}
` + skipQueriesPredicate + `
group by normalized_query_hash
`

	log.Info("Query Log Query", query)
	return query, nil
}

func getSkipQueryHashes(querySkipFile string, start, stop time.Time, conn driver.Conn) ([]uint64, error) {
	var skipQueryHashes []uint64

	skipQueries, err := loadSkipQueries(querySkipFile)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	query, err := buildQueryLogQuery(skipQueries)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	rows, err := conn.Query(
		context.Background(),
		query,
		clickhouse.Named("start", start.Format("2006-01-02")),
		clickhouse.Named("stop", stop.Format("2006-01-02")))
	if err != nil {
		log.Error(err, query)
		return nil, err
	}
	for rows.Next() {
		var (
			queryHash uint64
		)
		if err := rows.Scan(
			&queryHash,
		); err != nil {
			log.Error(err)
			return nil, err
		}
		skipQueryHashes = append(skipQueryHashes, queryHash)
	}
	return skipQueryHashes, nil
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
		errorStr := ""
		log.Println("worker", id, "started  job", q)
		err := conn.Exec(ctx, q.query)
		if err != nil {
			log.Warn(err)
			queryErrored = true
			errorStr = err.Error()
		}
		end := time.Now()
		queryResult := QueryResult{
			queryKind:          q.queryKind,
			originalStartTime:  q.queryStartTime,
			originalDurationMs: q.queryDurationMs,
			replayStartTime:    start,
			replayDurationMs:   uint64(end.Sub(start).Milliseconds()),
			deltaMs:            int64(q.queryDurationMs) - int64(end.Sub(start).Milliseconds()),
			queryErrored:       queryErrored,
			errorStr:           errorStr,
			query:              q.query,
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
				r.originalStartTime.Format(time.RFC3339Nano),
				strconv.FormatUint(r.originalDurationMs, 10),
				r.replayStartTime.Format(time.RFC3339Nano),
				strconv.FormatUint(r.replayDurationMs, 10),
				strconv.FormatInt(r.deltaMs, 10),
				strconv.FormatBool(r.queryErrored),
				r.errorStr,
				r.query,
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

func replayQueryHistory(ctx context.Context, fromConn, toConn driver.Conn, cluster string, start, stop time.Time, skip_file string) error {
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

	var skipHashQuery string

	if skip_file != "" {
		// load the skip queries
		skipHashes, err := getSkipQueryHashes(skip_file, start, stop, fromConn)
		if err != nil {
			log.Fatal(err)
		}

		skipHashesStr := []string{}
		for _, h := range skipHashes {
			skipHashesStr = append(skipHashesStr, strconv.FormatUint(h, 10))
		}

		skipHashQuery = `and normalized_query_hash not in (` + strings.Join(skipHashesStr, `, `) + `)`
	}

	query := `
		select query_kind, query, query_start_time_microseconds, query_duration_ms 
		from clusterAllReplicas({cluster:String}, system.query_log)
		where type = 2 and is_initial_query = 1 and query_kind = 'Select'
		and query_start_time >= {start:String} and query_start_time <= {stop:String}
		` + skipHashQuery + `		
		group by query, query_start_time_microseconds, query_duration_ms, query_kind
		order by query_start_time_microseconds asc
		`

	log.Infof("Replaying query history from %s to %s", start.Format("2006-01-02"), stop.Format("2006-01-02"))
	rows, err := fromConn.Query(ctx,
		query,
		clickhouse.Named("cluster", cluster),
		clickhouse.Named("start", start.Format("2006-01-02")),
		clickhouse.Named("stop", stop.Format("2006-01-02")))
	if err != nil {
		log.Fatal(query, err)
	}

	// Create a new CSV file
	file, err := os.Create("queries.csv")
	if err != nil {
		log.Fatalf("failed creating file: %s", err)
	}
	writer := csv.NewWriter(file)
	defer writer.Flush()

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
			log.Warn(err)
		}

		if err := writer.Write(
			[]string{
				queryKind,
				query,
				queryStartTime.Format(time.RFC3339Nano),
				strconv.FormatUint(queryDurationMs, 10),
			}); err != nil {
			log.Fatalln("error writing record to csv:", err)
		}
		if err != nil {
			log.Warn(err)
		}

		writer.Flush()
	}
	file.Close()

	file, err = os.OpenFile("queries.csv", os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Fatalf("failed opening file: %s", err)
	}
	defer file.Close()

	r := csv.NewReader(file)
	var tsOffset time.Duration
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Warn(err)
		}
		queryKind := record[0]
		query := record[1]
		queryStartTime, err := time.Parse(time.RFC3339Nano, record[2])
		if err != nil {
			log.Warn(err)
		}
		queryDurationMs, err := strconv.ParseUint(record[3], 10, 64)
		if err != nil {
			log.Warn(err)
		}

		queryRow := Query{
			queryKind:       queryKind,
			query:           query,
			queryStartTime:  queryStartTime,
			queryDurationMs: queryDurationMs,
		}
		if tsOffset == 0 {
			// this is the first loop - set a few ts vars
			tsOffset = time.Since(queryStartTime)
		}
		for {
			virtualTime := time.Now().UTC().Add(-tsOffset)
			log.Info("Virtual time: ", virtualTime, " Query start time: ", queryStartTime)
			if queryStartTime.Before(virtualTime) {
				wg.Add(1)
				queries <- queryRow
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	close(queries)
	wg.Wait()
	return nil
}
