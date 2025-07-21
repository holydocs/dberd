// Package plantuml provides functionality for converting database schemas into PlantUML ERD format.
// PlantUML is a widely used diagramming tool that supports Entity Relationship Diagrams (ERD).
// This package implements the dberd.Target interface for PlantUML diagram generation.
package plantuml

import (
	"bytes"
	"context"
	"embed"
	"errors"
	"fmt"
	"text/template"

	"github.com/holydocs/dberd"
)

// targetType defines the schema format type for PlantUML diagrams
const targetType = dberd.TargetType("plantuml")

//go:embed schema.tmpl
var templateFS embed.FS

// Ensure Target implements dberd interfaces.
var _ dberd.Target = (*Target)(nil)

// Target represents a PlantUML diagram formatter that converts database schemas into PlantUML format.
// It handles the conversion of database schemas to PlantUML ERD diagrams.
type Target struct {
	template *template.Template
}

// NewTarget creates a new PlantUML diagram formatter instance.
// It initializes the template from the embedded schema.tmpl file.
//
// Returns an error if the template parsing fails.
func NewTarget() (*Target, error) {
	tmpl, err := template.ParseFS(templateFS, "schema.tmpl")
	if err != nil {
		return nil, fmt.Errorf("parsing template: %w", err)
	}

	return &Target{
		template: tmpl,
	}, nil
}

// Capabilities returns target capabilities.
func (t *Target) Capabilities() dberd.TargetCapabilities {
	return dberd.TargetCapabilities{
		Format: true,
		Render: false,
	}
}

// FormatSchema converts a database schema into PlantUML ERD format.
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

// RenderSchema is unsupported for plantuml target.
func (t *Target) RenderSchema(_ context.Context, _ dberd.FormattedSchema) ([]byte, error) {
	return nil, errors.New("unsupported")
}
