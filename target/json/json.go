// Package json provides functionality for formatting database schemas as JSON.
package json

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/holydocs/dberd"
)

// targetType represents the JSON format type identifier.
const targetType = dberd.TargetType("json")

// Ensure Target implements dberd interfaces.
var (
	_ dberd.Target = (*Target)(nil)
)

// Target implements the schema formatting and rendering functionality for JSON format.
type Target struct {
}

// NewTarget creates and returns a new JSON target instance.
func NewTarget() *Target {
	return &Target{}
}

// Capabilities returns target capabilities.
func (t *Target) Capabilities() dberd.TargetCapabilities {
	return dberd.TargetCapabilities{
		Format: true,
		Render: false,
	}
}

// FormatSchema converts a database schema into a JSON-formatted representation.
// It returns a FormattedSchema containing the JSON data and format type.
func (t *Target) FormatSchema(_ context.Context, s dberd.Schema) (dberd.FormattedSchema, error) {
	jsonData, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return dberd.FormattedSchema{}, fmt.Errorf("marshalling schema to json: %w", err)
	}
	return dberd.FormattedSchema{
		Type: targetType,
		Data: jsonData,
	}, nil
}

// RenderSchema is unsupported for json target.
func (t *Target) RenderSchema(_ context.Context, _ dberd.FormattedSchema) ([]byte, error) {
	return nil, errors.New("unsupported")
}
