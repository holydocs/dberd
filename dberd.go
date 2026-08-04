// Package dberd provides functionality for database schema extraction, formatting, and rendering.
package dberd

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
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
			if s.Tables[i].Columns[j].Definition != s.Tables[i].Columns[k].Definition {
				return s.Tables[i].Columns[j].Definition < s.Tables[i].Columns[k].Definition
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

// AddLogicalReferences adds references inferred from <table>_id column names.
// Existing references take precedence, and targets must be in the same schema.
func (s *Schema) AddLogicalReferences() {
	type tableAlias struct {
		qualifier string
		name      string
	}

	referencedSources := make(map[TableColumn]struct{}, len(s.References))
	for _, reference := range s.References {
		referencedSources[reference.Source] = struct{}{}
	}

	targetsByAlias := make(map[tableAlias][]TableColumn)
	for _, table := range s.Tables {
		if !tableHasColumn(table, "id") {
			continue
		}

		qualifier, name := splitTableName(table.Name)
		target := TableColumn{Table: table.Name, Column: "id"}
		for _, alias := range logicalTableAliases(name) {
			key := tableAlias{qualifier: qualifier, name: alias}
			targetsByAlias[key] = append(targetsByAlias[key], target)
		}
	}

	for _, table := range s.Tables {
		qualifier, _ := splitTableName(table.Name)
		for _, column := range table.Columns {
			stem, ok := strings.CutSuffix(column.Name, "_id")
			if !ok || stem == "" {
				continue
			}

			source := TableColumn{Table: table.Name, Column: column.Name}
			if _, ok := referencedSources[source]; ok {
				continue
			}

			targets := targetsByAlias[tableAlias{qualifier: qualifier, name: stem}]
			if len(targets) != 1 {
				continue
			}

			s.References = append(s.References, Reference{Source: source, Target: targets[0]})
			referencedSources[source] = struct{}{}
		}
	}
}

func splitTableName(name string) (string, string) {
	separator := strings.LastIndexByte(name, '.')
	if separator == -1 {
		return "", name
	}

	return name[:separator], name[separator+1:]
}

func logicalTableAliases(name string) []string {
	if name == "" {
		return nil
	}

	aliases := []string{name}
	var singular string
	switch {
	case strings.HasSuffix(name, "ies"):
		singular = strings.TrimSuffix(name, "ies") + "y"
	case strings.HasSuffix(name, "s"):
		singular = strings.TrimSuffix(name, "s")
	}

	if singular != "" && singular != name {
		aliases = append(aliases, singular)
	}

	return aliases
}

func tableHasColumn(table Table, name string) bool {
	for _, column := range table.Columns {
		if column.Name == name {
			return true
		}
	}

	return false
}

// SkipTables removes tables with the given names and all references to them.
// An unqualified name matches tables in every schema, while a qualified name
// matches only the exact table name. Names are case-sensitive; surrounding
// whitespace and empty names are ignored.
func (s *Schema) SkipTables(names ...string) {
	skipped := make(map[string]struct{}, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name != "" {
			skipped[name] = struct{}{}
		}
	}

	if len(skipped) == 0 {
		return
	}

	if s.Tables != nil {
		tables := make([]Table, 0, len(s.Tables))
		for _, table := range s.Tables {
			if !isSkippedTable(table.Name, skipped) {
				tables = append(tables, table)
			}
		}
		s.Tables = tables
	}

	if s.References != nil {
		references := make([]Reference, 0, len(s.References))
		for _, reference := range s.References {
			if !isSkippedTable(reference.Source.Table, skipped) &&
				!isSkippedTable(reference.Target.Table, skipped) {
				references = append(references, reference)
			}
		}
		s.References = references
	}
}

func isSkippedTable(table string, skipped map[string]struct{}) bool {
	if _, ok := skipped[table]; ok {
		return true
	}

	baseName := table
	if separator := strings.LastIndexByte(table, '.'); separator >= 0 {
		baseName = table[separator+1:]
	}

	_, ok := skipped[baseName]
	return ok
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
