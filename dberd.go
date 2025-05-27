// Package dberd provides functionality for database schema extraction, formatting, and rendering.
package dberd

import (
	"context"
	"fmt"
	"io"
	"sort"
)

// TargetType represents the type of language for describing database schema.
type TargetType string

// Schema represents a complete database schema with tables and their references.
type Schema struct {
	Tables     []Table     `json:"tables"`
	References []Reference `json:"references"`
}

// Sort sorts the schema's tables and references in a consistent order.
func (s *Schema) Sort() {
	sort.Slice(s.Tables, func(i, j int) bool {
		return s.Tables[i].Name < s.Tables[j].Name
	})

	for i := range s.Tables {
		sort.Slice(s.Tables[i].Columns, func(j, k int) bool {
			if s.Tables[i].Columns[j].IsPrimary != s.Tables[i].Columns[k].IsPrimary {
				return s.Tables[i].Columns[j].IsPrimary
			}
			return s.Tables[i].Columns[j].Name < s.Tables[i].Columns[k].Name
		})
	}

	sort.Slice(s.References, func(i, j int) bool {
		switch {
		case s.References[i].Source.Table != s.References[j].Source.Table:
			return s.References[i].Source.Table < s.References[j].Source.Table
		case s.References[i].Source.Column != s.References[j].Source.Column:
			return s.References[i].Source.Column < s.References[j].Source.Column
		case s.References[i].Target.Table != s.References[j].Target.Table:
			return s.References[i].Target.Table < s.References[j].Target.Table
		default:
			return s.References[i].Target.Column < s.References[j].Target.Column
		}
	})
}

// Table represents a database table with its columns.
type Table struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
}

// Column represents a database table column.
type Column struct {
	Name       string `json:"name"`
	Comment    string `json:"comment,omitempty"`
	Definition string `json:"definition"`
	IsPrimary  bool   `json:"is_primary"`
}

// TableColumn represents a reference to a specific column in a table.
type TableColumn struct {
	Table  string `json:"table"`
	Column string `json:"column"`
}

// Reference represents a foreign key relationship between two table columns.
type Reference struct {
	Source TableColumn `json:"source"`
	Target TableColumn `json:"target"`
}

// FormattedSchema represents a formatted database schema.
type FormattedSchema struct {
	Type TargetType `json:"type"`
	Data []byte     `json:"data"`
}

// TargetCapabilities represents the capabilities of a Target implementation.
type TargetCapabilities struct {
	Format bool
	Render bool
}

// Source defines the interface for database schema sources that can extract schema information.
type Source interface {
	SchemaExtractor
	io.Closer
}

// Target defines the interface for database schema targets that can format and render schema information.
type Target interface {
	SchemaFormatter
	SchemaRenderer
	Capabilities() TargetCapabilities
}

// SchemaExtractor defines the interface for extracting database schema.
type SchemaExtractor interface {
	ExtractSchema(ctx context.Context) (Schema, error)
}

// SchemaFormatter defines the interface for formatting database schema.
type SchemaFormatter interface {
	FormatSchema(ctx context.Context, s Schema) (FormattedSchema, error)
}

// SchemaRenderer defines the interface for rendering formatted database schema.
type SchemaRenderer interface {
	RenderSchema(ctx context.Context, fs FormattedSchema) ([]byte, error)
}

// UnsupportedFormatError represents an error when an unsupported format is provided.
type UnsupportedFormatError struct {
	given    TargetType
	expected TargetType
}

// NewUnsupportedFormatError creates a new UnsupportedFormatError.
func NewUnsupportedFormatError(given, expected TargetType) error {
	return &UnsupportedFormatError{
		given:    given,
		expected: expected,
	}
}

// Error implements the error interface for UnsupportedFormatError.
func (err *UnsupportedFormatError) Error() string {
	return fmt.Sprintf("%s format is not supported, %s expected", err.given, err.expected)
}
