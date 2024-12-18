package main

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

type Options struct {
	DB             *sql.DB
	DB2            *sql.DB
	Path           string
	SpecifiedDB    string
	TableNamesOnly bool
	NoKafkas       bool
	NoMatViews     bool
	OnlyKafkas     bool
	OnlyMatViews   bool
	Apply          bool
	IfNotExists    bool
}

var (
	tableEngines = []string{
		"%MergeTree",
		"Kafka",
		"Distributed",
		"Dictionary",
		"Join",
		"MaterializedView",
		"View",
	}
)

func Compare(opts *Options) error {
	var err error

	databases, err := validateDatabase(opts)
	if err != nil {
		return err
	}

	for _, dbName := range databases {
		if dbName == "system" {
			continue
		}

		// Get source tables
		var tables []string
		tables, err := getTables(opts.DB, dbName)
		if err != nil {
			return err
		}

		// Get DB2 tables
		var tables2 []string
		tables2, err = getTables(opts.DB2, dbName)
		if err != nil {
			return err
		}

		for _, tableName := range tables {
			if !includes(tables2, tableName) {
				fmt.Printf("-- Table '%s.%s' is missing in the destination\n", dbName, tableName)
				if !opts.TableNamesOnly {
					tableCreateStmt, err := fetchTableCreateStmt(opts.DB, dbName, tableName, opts.IfNotExists)
					if err != nil {
						return err
					}
					if opts.Apply {
						_, err = opts.DB2.Exec(tableCreateStmt)
						if err != nil {
							log.Errorf("applying table '%s.%s' create statement: %v", dbName, tableName, err)
							continue
						}
						fmt.Printf("Applied table '%s.%s' to second ClickHouse instance\n", dbName, tableName)
					} else {
						fmt.Printf("%s;\n\n", tableCreateStmt)
					}
				}
			}
		}
	}

	return nil
}

func Write(opts *Options) error {
	var fd *os.File
	var err error
	if opts.NoKafkas {
		tableEngines = removeElement(tableEngines, "Kafka")
	}

	if opts.NoMatViews {
		tableEngines = removeElement(tableEngines, "MaterializedView")
	}

	if opts.OnlyKafkas && opts.OnlyMatViews {
		return fmt.Errorf("can't specify both --only-kafkas and --only-mat-views")
	}

	if opts.OnlyKafkas {
		tableEngines = []string{"Kafka"}
	}

	if opts.OnlyMatViews {
		tableEngines = []string{"MaterializedView"}
	}

	if len(opts.Path) > 0 {
		fd, err = os.OpenFile(opts.Path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return fmt.Errorf("opening file: %v", err)
		}
		defer fd.Close()
	} else {
		fd = os.Stdout
	}

	databases, err := validateDatabase(opts)
	if err != nil {
		return err
	}

	for _, dbName := range databases {
		if dbName == "system" {
			continue
		}
		dbCreateStmt, err := dbCreateStmt(opts.DB, dbName)
		if err != nil {
			return err
		}
		_, err = fd.Write([]byte(dbCreateStmt + ";\n\n"))
		if err != nil {
			return fmt.Errorf("writing database '%s' create statement: %v", dbName, err)
		}

		var tables []string
		for _, engine := range tableEngines {
			newTables, err := getTablesByEngine(opts.DB, dbName, engine)
			if err != nil {
				return err
			}
			tables = append(tables, newTables...)
		}
		for _, tableName := range tables {
			tableCreateStmt, err := fetchTableCreateStmt(opts.DB, dbName, tableName, opts.IfNotExists)
			if err != nil {
				return err
			}
			_, err = fd.Write([]byte(tableCreateStmt + ";\n\n"))
			if err != nil {
				return fmt.Errorf("writing table '%s' create statement: %v", tableName, err)
			}
		}
	}

	return nil
}

func getDatabases(db *sql.DB) ([]string, error) {
	var databases []string
	rows, err := db.Query("SHOW DATABASES FORMAT TabSeparated;")
	if err != nil {
		return []string{}, fmt.Errorf("getting databases: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return []string{}, fmt.Errorf("getting databases: %v", err)
		}
		databases = append(databases, name)
	}

	if err = rows.Err(); err != nil {
		return []string{}, fmt.Errorf("getting databases: %v", err)
	}

	return databases, nil
}

func getTables(db *sql.DB, dbName string) ([]string, error) {
	var tables []string
	rows, err := db.Query("SELECT name FROM system.tables WHERE name not like '.inner_id.%' AND database = ?;", dbName)
	if err != nil {
		return []string{}, fmt.Errorf("getting tables for '%s': %v", dbName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return []string{}, fmt.Errorf("getting tables for '%s': %v", dbName, err)
		}
		tables = append(tables, name)
	}

	if err = rows.Err(); err != nil {
		return []string{}, fmt.Errorf("getting tables for '%s': %v", dbName, err)
	}

	return tables, nil
}

func getTablesByEngine(db *sql.DB, dbName string, engineFilter string) ([]string, error) {
	var tables []string
	rows, err := db.Query("SELECT name FROM system.tables WHERE name not like '.inner_id.%' AND database = ? AND engine like ?;", dbName, engineFilter)
	if err != nil {
		return []string{}, fmt.Errorf("getting tables for '%s': %v", dbName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return []string{}, fmt.Errorf("getting tables for '%s': %v", dbName, err)
		}
		if !(len(name) > 6 && name[:7] == ".inner.") {
			tables = append(tables, name)
		}
	}

	if err =rows.Err(); err != nil {
		return []string{}, fmt.Errorf("getting tables for '%s': %v", dbName, err)
	}

	return tables, nil
}

func dbCreateStmt(db *sql.DB, dbName string) (string, error) {
	var createStmt string
	queryStmt := fmt.Sprintf("SHOW CREATE DATABASE %s FORMAT PrettySpaceNoEscapes;", dbName)
	err := db.QueryRow(queryStmt).Scan(&createStmt)
	if err != nil {
		return "", fmt.Errorf("getting database %s create statement: %v", dbName, err)
	}

	return strings.Replace(createStmt, "CREATE DATABASE", "CREATE DATABASE IF NOT EXISTS", 1), nil
}

func fetchTableCreateStmt(db *sql.DB, dbName string, tableName string, ifNotExists bool) (string, error) {
	var createStmt string
	queryStmt := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s` FORMAT PrettySpaceNoEscapes;", dbName, tableName)
	err := db.QueryRow(queryStmt).Scan(&createStmt)
	if err != nil {
		return "", fmt.Errorf("getting table '%s.%s' statement: %v", dbName, tableName, err)
	}

	if ifNotExists {
		createStmt = strings.Replace(createStmt, "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
		createStmt = strings.Replace(createStmt, "CREATE VIEW", "CREATE VIEW IF NOT EXISTS", 1)
		createStmt = strings.Replace(createStmt, "CREATE DICTIONARY", "CREATE DICTIONARY IF NOT EXISTS", 1)
		createStmt = strings.Replace(createStmt, "CREATE MATERIALIZED VIEW", "CREATE MATERIALIZED VIEW IF NOT EXISTS", 1)
	}

	return createStmt, nil
}

func includes(strs []string, str string) bool {
	for _, s := range strs {
		if s == str {
			return true
		}
	}

	return false
}

func validateDatabase(opts *Options) ([]string, error) {
	allDatabases, err := getDatabases(opts.DB)
	if err != nil {
		return nil, fmt.Errorf("getting databases: %v", err)
	}

	var databases []string
	switch opts.SpecifiedDB {
	case "":
		databases = allDatabases
	case "system":
		return nil, fmt.Errorf("'%s' is a special internal ClickHouse database and can't be specified", opts.SpecifiedDB)
	default:
		if includes(allDatabases, opts.SpecifiedDB) {
			databases = []string{opts.SpecifiedDB}
		} else {
			return nil, fmt.Errorf("specified database '%s' doesnt exist", opts.SpecifiedDB)
		}
	}

	if opts.DB2 != nil {
		allDatabases2, err := getDatabases(opts.DB2)
		if err != nil {
			return nil, fmt.Errorf("getting databases: %v", err)
		}

		if opts.SpecifiedDB != "" {
			if includes(allDatabases2, opts.SpecifiedDB) {
				databases = []string{opts.SpecifiedDB}
			} else {
				return nil, fmt.Errorf("specified database '%s' doesnt exist", opts.SpecifiedDB)
			}
		} else {
			databases = allDatabases2
		}
	}
	return databases, nil
}
