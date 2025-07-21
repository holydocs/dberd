// Package cockroach provides functionality for extracting database schema information
// from CockroachDB databases.
package cockroach

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/holydocs/dberd"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// Ensure Source implements dberd interfaces.
var (
	_ dberd.Source = (*Source)(nil)
)

// Source represents a CockroachDB database source for schema extraction.
// It maintains a database connection and implements the dberd.SchemaExtractor interface
// to provide schema information from a CockroachDB instance.
type Source struct {
	db     *sql.DB
	closer io.Closer
}

// NewSource creates a new CockroachDB source from a connection string.
// It parses the connection string, establishes a database connection,
// and returns a new Source instance ready for schema extraction.
func NewSource(connStr string) (*Source, error) {
	cockroachConfig, err := pgx.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("parsing cockroach connection string: %w", err)
	}

	cockroachConnector := stdlib.GetConnector(*cockroachConfig)
	db := sql.OpenDB(cockroachConnector)

	return &Source{
		db:     db,
		closer: db,
	}, nil
}

// NewSourceFromDB creates a new CockroachDB source from an existing database connection.
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
// It returns a dberd.Schema containing all tables and their relationships.
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
	WITH pk_columns AS (
    	SELECT 
    	    kcu.table_schema,
    	    kcu.table_name,
    	    kcu.column_name
    	FROM information_schema.key_column_usage kcu
    	JOIN information_schema.table_constraints tc
    	    ON tc.constraint_name = kcu.constraint_name
    	    AND tc.table_schema = kcu.table_schema
    	WHERE tc.constraint_type = 'PRIMARY KEY'
    	ORDER BY kcu.table_schema, kcu.table_name, kcu.column_name
	)
	SELECT
	    c.table_schema,
	    c.table_name,
	    c.column_name,
	    c.crdb_sql_type AS data_type,
	    c.is_nullable,
	    c.column_default,
	    c.column_comment,
	    EXISTS (
	        SELECT 1 
	        FROM pk_columns pk 
	        WHERE pk.table_schema = c.table_schema 
	        AND pk.table_name = c.table_name 
	        AND pk.column_name = c.column_name
	    ) AS is_primary
	FROM information_schema.columns c
	JOIN information_schema.tables t ON c.table_schema = t.table_schema AND c.table_name = t.table_name
	WHERE c.table_schema IN (SELECT schema_name FROM information_schema.schemata WHERE crdb_is_user_defined = 'YES')
	AND is_hidden = 'NO'
	AND t.table_type = 'BASE TABLE'
	ORDER BY c.table_schema, c.table_name, c.ordinal_position;`

type tableRow struct {
	tableSchema   string
	tableName     string
	columnName    string
	dataType      string
	isNullable    string
	columnDefault *string
	columnComment *string
	isPrimary     bool
}

// extractTables queries the database for table and column information and converts it to dberd.Table format.
// It excludes system schemas and hidden columns.
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
			&r.dataType,
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
// It groups columns by table and constructs table definitions with their columns.
func tableRowsToSchemaTables(tableRows []tableRow) []dberd.Table {
	// Pre-allocate map with estimated size
	tableMap := make(map[string]*dberd.Table, len(tableRows)/10) // Assuming average 10 columns per table

	for _, row := range tableRows {
		tableKey := row.tableSchema + "." + row.tableName

		table, exists := tableMap[tableKey]
		if !exists {
			table = &dberd.Table{
				Name:    tableKey,
				Columns: make([]dberd.Column, 0, 10), // Pre-allocate for average column count
			}
			tableMap[tableKey] = table
		}

		definition := row.dataType
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

		if row.columnComment != nil {
			column.Comment = *row.columnComment
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
	WITH foreign_keys AS (
		SELECT
			src_ns.nspname AS source_schema,
			src_tbl.relname AS source_table,
			src_col.attname AS source_column,
			tgt_ns.nspname AS target_schema,
			tgt_tbl.relname AS target_table,
			tgt_col.attname AS target_column,
			ROW_NUMBER() OVER (
				PARTITION BY src_ns.nspname, src_tbl.relname, src_col.attname
				ORDER BY tgt_ns.nspname, tgt_tbl.relname, tgt_col.attname
			) as rn
		FROM pg_constraint con
		JOIN pg_class src_tbl ON con.conrelid = src_tbl.oid
		JOIN pg_namespace src_ns ON src_tbl.relnamespace = src_ns.oid
		JOIN pg_class tgt_tbl ON con.confrelid = tgt_tbl.oid
		JOIN pg_namespace tgt_ns ON tgt_tbl.relnamespace = tgt_ns.oid
		JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS src_cols(attnum, ord) ON TRUE
		JOIN pg_attribute src_col ON src_col.attrelid = src_tbl.oid AND src_col.attnum = src_cols.attnum
		JOIN LATERAL unnest(con.confkey) WITH ORDINALITY AS tgt_cols(attnum, ord) ON src_cols.ord = tgt_cols.ord
		JOIN pg_attribute tgt_col ON tgt_col.attrelid = tgt_tbl.oid AND tgt_col.attnum = tgt_cols.attnum
		WHERE con.contype = 'f'
	)
	SELECT 
		source_schema,
		source_table,
		source_column,
		target_schema,
		target_table,
		target_column
	FROM foreign_keys
	WHERE rn = 1
	ORDER BY source_schema, source_table, source_column;`

type referenceRow struct {
	sourceSchema string
	sourceTable  string
	sourceColumn string
	targetSchema string
	targetTable  string
	targetColumn string
}

// extractReferences queries the database for foreign key relationships and converts them to dberd.Reference format.
func (s *Source) extractReferences(ctx context.Context) ([]dberd.Reference, error) {
	rows, err := s.db.QueryContext(ctx, extractReferencesQuery)
	if err != nil {
		return nil, fmt.Errorf("querying references: %w", err)
	}
	defer rows.Close()

	var referenceRows []referenceRow

	for rows.Next() {
		var r referenceRow
		if err := rows.Scan(
			&r.sourceSchema,
			&r.sourceTable,
			&r.sourceColumn,
			&r.targetSchema,
			&r.targetTable,
			&r.targetColumn,
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
// It constructs references between tables by combining schema and table names.
func referenceRowsToSchemaReferences(referenceRows []referenceRow) []dberd.Reference {
	// Pre-allocate slice with exact size
	references := make([]dberd.Reference, 0, len(referenceRows))

	for _, row := range referenceRows {
		sourceTable := row.sourceSchema + "." + row.sourceTable
		targetTable := row.targetSchema + "." + row.targetTable

		references = append(references, dberd.Reference{
			Source: dberd.TableColumn{
				Table:  sourceTable,
				Column: row.sourceColumn,
			},
			Target: dberd.TableColumn{
				Table:  targetTable,
				Column: row.targetColumn,
			},
		})
	}

	return references
}
