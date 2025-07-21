// Package clickhouse provides functionality for extracting database schema information
// from ClickHouse databases.
package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	_ "github.com/ClickHouse/clickhouse-go/v2" // import clickhouse driver
	"github.com/holydocs/dberd"
)

// Ensure Source implements dberd interfaces.
var (
	_ dberd.Source = (*Source)(nil)
)

// Source represents a ClickHouse database source for schema extraction.
type Source struct {
	db     *sql.DB
	closer io.Closer
}

// NewSource creates a new ClickHouse source from a connection string.
func NewSource(connStr string) (*Source, error) {
	db, err := sql.Open("clickhouse", connStr)
	if err != nil {
		return nil, fmt.Errorf("opening sql connection: %w", err)
	}

	return &Source{
		db:     db,
		closer: db,
	}, nil
}

// NewSourceFromDB creates a new ClickHouse source from an existing database connection.
// This is useful when you want to reuse an existing database connection
// for schema extraction purposes.
func NewSourceFromDB(db *sql.DB) *Source {
	return &Source{
		db: db,
	}
}

// Close closes the database connection if it was created by NewSource.
// If the connection was provided externally (via NewSourceFromDB), this is a no-op.
func (s *Source) Close() error {
	if s.closer == nil {
		return nil
	}

	return s.closer.Close()
}

// ExtractSchema extracts the complete database schema including tables.
// It returns a dberd.Schema containing all tables.
func (s *Source) ExtractSchema(ctx context.Context) (schema dberd.Schema, err error) {
	schema.Tables, err = s.extractTables(ctx)
	if err != nil {
		return dberd.Schema{}, fmt.Errorf("extracting tables: %w", err)
	}

	return schema, nil
}

const extractTablesQuery = `
	SELECT
		database,
		table,
		name,
		type,
		default_expression,
		comment,
		is_in_primary_key
	FROM system.columns
	WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
	ORDER BY database, name, position;`

type tableRow struct {
	database          string
	tableName         string
	columnName        string
	dataType          string
	defaultExpression *string
	comment           *string
	isPrimary         bool
}

// extractTables queries the database for table and column information and converts it to dberd.Table format.
// It excludes system databases.
func (s *Source) extractTables(ctx context.Context) ([]dberd.Table, error) {
	rows, err := s.db.QueryContext(ctx, extractTablesQuery)
	if err != nil {
		return nil, fmt.Errorf("querying tables: %w", err)
	}
	defer rows.Close()

	tablesRows := make([]tableRow, 0, 100) // Assuming tables rows.

	for rows.Next() {
		var r tableRow
		if err := rows.Scan(
			&r.database,
			&r.tableName,
			&r.columnName,
			&r.dataType,
			&r.defaultExpression,
			&r.comment,
			&r.isPrimary,
		); err != nil {
			return nil, fmt.Errorf("scanning tables row: %w", err)
		}

		tablesRows = append(tablesRows, r)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("tables rows error: %w", err)
	}

	return tableRowsToSchemaTables(tablesRows), nil
}

// tableRowsToSchemaTables converts a slice of tableRow into a slice of dberd.Table.
// It groups columns by table and constructs table definitions with their columns.
func tableRowsToSchemaTables(tableRows []tableRow) []dberd.Table {
	// Pre-allocate map with estimated size
	tableMap := make(map[string]*dberd.Table, len(tableRows)/10) // Assuming average 10 columns per table

	for _, row := range tableRows {
		tableKey := row.database + "." + row.tableName

		table, exists := tableMap[tableKey]
		if !exists {
			table = &dberd.Table{
				Name:    tableKey,
				Columns: make([]dberd.Column, 0, 10), // Pre-allocate for average column count
			}
			tableMap[tableKey] = table
		}

		definition := row.dataType
		if row.defaultExpression != nil && *row.defaultExpression != "" {
			definition += " DEFAULT " + *row.defaultExpression
		}

		column := dberd.Column{
			Name:       row.columnName,
			Definition: definition,
			IsPrimary:  row.isPrimary,
		}

		if row.comment != nil {
			column.Comment = *row.comment
		}

		table.Columns = append(table.Columns, column)
	}

	// Pre-allocate slice with exact size
	tables := make([]dberd.Table, 0, len(tableMap))
	for _, table := range tableMap {
		tables = append(tables, *table)
	}

	return tables
}
