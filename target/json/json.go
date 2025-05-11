// Package json provides functionality for formatting database schemes as JSON.
package json

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/denchenko/dberd"
)

// targetType represents the JSON format type identifier.
const targetType = dberd.TargetType("json")

// Ensure Target implements dberd interfaces.
var (
	_ dberd.Target = (*Target)(nil)
)

// Target implements the scheme formatting and rendering functionality for JSON format.
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

// FormatScheme converts a database scheme into a JSON-formatted representation.
// It returns a FormattedScheme containing the JSON data and format type.
func (t *Target) FormatScheme(_ context.Context, s dberd.Scheme) (dberd.FormattedScheme, error) {
	jsonData, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return dberd.FormattedScheme{}, fmt.Errorf("mashalling scheme to json: %w", err)
	}
	return dberd.FormattedScheme{
		Type: targetType,
		Data: jsonData,
	}, nil
}

// RenderScheme is unsupported for json target.
func (t *Target) RenderScheme(_ context.Context, _ dberd.FormattedScheme) ([]byte, error) {
	return nil, errors.New("unsupported")
}
