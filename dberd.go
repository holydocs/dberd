// Package dberd provides functionality for database schema extraction, formatting, and rendering.
package dberd

import (
	"context"
	"fmt"
	"io"
)

// TargetType represents the type of language for describing database schema.
type TargetType string

// Scheme represents a complete database schema with tables and their references.
type Scheme struct {
	Tables     []Table     `json:"tables"`
	References []Reference `json:"references"`
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

// FormattedScheme represents a formatted database schema.
type FormattedScheme struct {
	Type TargetType `json:"type"`
	Data []byte     `json:"data"`
}

// Source defines the interface for database schema sources that can extract schema information.
type Source interface {
	SchemeExtractor
	io.Closer
}

// Target defines the interface for database schema targets that can format and render schema information.
type Target interface {
	SchemeFormatter
	SchemeRenderer

	Capabilities() TargetCapabilities
}

// TargetCapabilities represents the capabilities of a Target implementation.
type TargetCapabilities struct {
	Format bool
	Render bool
}

// SchemeExtractor defines the interface for extracting database schema.
type SchemeExtractor interface {
	ExtractScheme(ctx context.Context) (Scheme, error)
}

// SchemeFormatter defines the interface for formatting database schema.
type SchemeFormatter interface {
	FormatScheme(ctx context.Context, s Scheme) (FormattedScheme, error)
}

// SchemeRenderer defines the interface for rendering formatted database schema.
type SchemeRenderer interface {
	RenderScheme(ctx context.Context, fs FormattedScheme) ([]byte, error)
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
