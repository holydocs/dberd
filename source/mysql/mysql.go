// Package mysql provides functionality for extracting database schema information
// from MySQL databases.
package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/denchenko/dberd"
	_ "github.com/go-sql-driver/mysql" // import mysql driver
)

// Ensure Source implements dberd interfaces.
var (
	_ dberd.Source = (*Source)(nil)
)

// Source represents a MySQL database source for schema extraction.
type Source struct {
	db     *sql.DB
	closer io.Closer
}

// NewSource creates a new MySQL source from a connection string.
func NewSource(connStr string) (*Source, error) {
	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return nil, fmt.Errorf("opening mysql connection: %w", err)
	}

	return &Source{
		db:     db,
		closer: db,
	}, nil
}

// NewSourceFromDB creates a new MySQL source from an existing database connection.
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

// ExtractSchema extracts the complete database schema including tables and their references.
func (s *Source) ExtractSchema(ctx context.Context) (schema dberd.Schema, err error) {
	schema.Tables, err = s.extractTables(ctx)
	if err != nil {
		return dberd.Schema{}, fmt.Errorf("extracting tables: %w", err)
	}

	schema.References, err = s.extractReferences(ctx)
	if err != nil {
		return dberd.Schema{}, fmt.Errorf("extracting references: %w", err)
	}

	return schema, nil
}

const extractTablesQuery = `
	SELECT 
		TABLE_SCHEMA,
		TABLE_NAME,
		COLUMN_NAME,
		COLUMN_TYPE,
		IS_NULLABLE,
		COLUMN_DEFAULT,
		COLUMN_COMMENT,
		COLUMN_KEY = 'PRI' as is_primary
	FROM information_schema.COLUMNS
	WHERE TABLE_SCHEMA NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')
	ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;`

type tableRow struct {
	tableSchema   string
	tableName     string
	columnName    string
	columnType    string
	isNullable    string
	columnDefault *string
	columnComment string
	isPrimary     bool
}

// extractTables queries the database for table and column information and converts it to dberd.Table format.
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
			&r.tableSchema,
			&r.tableName,
			&r.columnName,
			&r.columnType,
			&r.isNullable,
			&r.columnDefault,
			&r.columnComment,
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
func tableRowsToSchemaTables(tableRows []tableRow) []dberd.Table {
	// Pre-allocate map with estimated size
	tableMap := make(map[string]*dberd.Table, len(tableRows)/10) // Assuming average 10 columns per table

	for _, row := range tableRows {
		tableKey := row.tableSchema + "." + row.tableName

		table, exists := tableMap[tableKey]
		if !exists {
			table = &dberd.Table{
				Name:    tableKey,
				Columns: make([]dberd.Column, 0, 10),
			}
			tableMap[tableKey] = table
		}

		definition := row.columnType
		if row.isNullable == "NO" {
			definition += " NOT NULL"
		}
		if row.columnDefault != nil && *row.columnDefault != "" {
			definition += " DEFAULT " + *row.columnDefault
		}

		column := dberd.Column{
			Name:       row.columnName,
			Definition: definition,
			IsPrimary:  row.isPrimary,
		}

		if row.columnComment != "" {
			column.Comment = row.columnComment
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

const extractReferencesQuery = `
	SELECT 
		TABLE_SCHEMA,
		TABLE_NAME,
		COLUMN_NAME,
		REFERENCED_TABLE_SCHEMA,
		REFERENCED_TABLE_NAME,
		REFERENCED_COLUMN_NAME
	FROM information_schema.KEY_COLUMN_USAGE
	WHERE REFERENCED_TABLE_SCHEMA IS NOT NULL
	AND TABLE_SCHEMA NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')
	ORDER BY TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME;`

type referenceRow struct {
	tableSchema         string
	tableName           string
	columnName          string
	referencedSchema    string
	referencedTableName string
	referencedColumn    string
}

// extractReferences queries the database for foreign key relationships and converts them to dberd.Reference format.
func (s *Source) extractReferences(ctx context.Context) ([]dberd.Reference, error) {
	rows, err := s.db.QueryContext(ctx, extractReferencesQuery)
	if err != nil {
		return nil, fmt.Errorf("querying references: %w", err)
	}
	defer rows.Close()

	referenceRows := make([]referenceRow, 0, 50) // Assuming reasonable number of references

	for rows.Next() {
		var r referenceRow
		if err := rows.Scan(
			&r.tableSchema,
			&r.tableName,
			&r.columnName,
			&r.referencedSchema,
			&r.referencedTableName,
			&r.referencedColumn,
		); err != nil {
			return nil, fmt.Errorf("scanning references row: %w", err)
		}

		referenceRows = append(referenceRows, r)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("references rows error: %w", err)
	}

	return referenceRowsToSchemaReferences(referenceRows), nil
}

// referenceRowsToSchemaReferences converts a slice of referenceRow into a slice of dberd.Reference.
func referenceRowsToSchemaReferences(referenceRows []referenceRow) []dberd.Reference {
	references := make([]dberd.Reference, 0, len(referenceRows))

	for _, row := range referenceRows {
		reference := dberd.Reference{
			Source: dberd.TableColumn{
				Table:  row.tableSchema + "." + row.tableName,
				Column: row.columnName,
			},
			Target: dberd.TableColumn{
				Table:  row.referencedSchema + "." + row.referencedTableName,
				Column: row.referencedColumn,
			},
		}
		references = append(references, reference)
	}

	return references
}
