// Package mermaid provides functionality for converting database schemas into Mermaid JS ERD format.
package mermaid

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"strings"
	"text/template"

	"github.com/holydocs/dberd"
)

// targetType defines the schema format type for Mermaid JS diagrams
const targetType = dberd.TargetType("mermaid")

//go:embed schema.tmpl
var templateFS embed.FS

// Ensure Target implements dberd interfaces.
var _ dberd.Target = (*Target)(nil)

// Target represents a Mermaid JS diagram formatter that converts database schemas into Mermaid JS format.
type Target struct {
	template *template.Template
}

// NewTarget creates a new Mermaid JS diagram formatter instance.
func NewTarget() (*Target, error) {
	tmpl, err := template.New("schema.tmpl").Funcs(template.FuncMap{
		"mermaidComment": mermaidComment,
	}).ParseFS(templateFS, "schema.tmpl")
	if err != nil {
		return nil, fmt.Errorf("parsing template: %w", err)
	}

	return &Target{
		template: tmpl,
	}, nil
}

func mermaidComment(definition, comment string) string {
	parts := make([]string, 0, 2)

	for _, value := range []string{definition, comment} {
		value = strings.Join(strings.Fields(value), " ")
		value = strings.ReplaceAll(value, `"`, "'")
		if value != "" {
			parts = append(parts, value)
		}
	}

	return strings.Join(parts, "; ")
}

// Capabilities returns target capabilities.
func (t *Target) Capabilities() dberd.TargetCapabilities {
	return dberd.TargetCapabilities{
		Format: true,
		Render: false,
	}
}

// FormatSchema converts a database schema into Mermaid JS ERD format.
func (t *Target) FormatSchema(_ context.Context, s dberd.Schema) (dberd.FormattedSchema, error) {
	fs := dberd.FormattedSchema{
		Type: targetType,
	}

	var buf bytes.Buffer

	err := t.template.Execute(&buf, s)
	if err != nil {
		return dberd.FormattedSchema{}, fmt.Errorf("executing template: %w", err)
	}

	fs.Data = buf.Bytes()

	return fs, nil
}

// RenderSchema is unsupported for mermaid target.
func (t *Target) RenderSchema(_ context.Context, _ dberd.FormattedSchema) ([]byte, error) {
	return nil, fmt.Errorf("unsupported")
}
