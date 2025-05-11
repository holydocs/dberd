// Package plantuml provides functionality for converting database schemes into PlantUML ERD format.
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

	"github.com/denchenko/dberd"
)

// targetType defines the scheme format type for PlantUML diagrams
const targetType = dberd.TargetType("plantuml")

//go:embed scheme.tmpl
var templateFS embed.FS

// Ensure Target implements dberd interfaces.
var _ dberd.Target = (*Target)(nil)

// Target represents a PlantUML diagram formatter that converts database schemes into PlantUML format.
// It handles the conversion of database schemes to PlantUML ERD diagrams.
type Target struct {
	template *template.Template
}

// NewTarget creates a new PlantUML diagram formatter instance.
// It initializes the template from the embedded scheme.tmpl file.
//
// Returns an error if the template parsing fails.
func NewTarget() (*Target, error) {
	tmpl, err := template.ParseFS(templateFS, "scheme.tmpl")
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

// FormatScheme converts a database scheme into PlantUML ERD format.
func (t *Target) FormatScheme(_ context.Context, s dberd.Scheme) (dberd.FormattedScheme, error) {
	fs := dberd.FormattedScheme{
		Type: targetType,
	}

	var buf bytes.Buffer

	err := t.template.Execute(&buf, s)
	if err != nil {
		return dberd.FormattedScheme{}, fmt.Errorf("executing template: %w", err)
	}

	fs.Data = buf.Bytes()

	return fs, nil
}

// RenderScheme is unsupported for plantuml target.
func (t *Target) RenderScheme(_ context.Context, _ dberd.FormattedScheme) ([]byte, error) {
	return nil, errors.New("unsupported")
}
