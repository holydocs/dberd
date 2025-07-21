// Package d2 provides functionality for converting database schemas into D2 diagram format.
// D2 is a modern diagram scripting language that turns text into diagrams.
// This package implements the dberd.Target interface for D2 diagram generation.
package d2

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"text/template"

	"github.com/holydocs/dberd"
	"oss.terrastruct.com/d2/d2graph"
	"oss.terrastruct.com/d2/d2layouts/d2elklayout"
	"oss.terrastruct.com/d2/d2lib"
	"oss.terrastruct.com/d2/d2renderers/d2svg"
	"oss.terrastruct.com/d2/d2themes/d2themescatalog"
	"oss.terrastruct.com/d2/lib/log"
	"oss.terrastruct.com/d2/lib/textmeasure"
	"oss.terrastruct.com/util-go/go2"
)

// targetType defines the schema format type for D2 diagrams
const targetType = dberd.TargetType("d2")

//go:embed schema.tmpl
var templateFS embed.FS

// Ensure Target implements dberd interfaces.
var (
	_ dberd.Target = (*Target)(nil)
)

// Target represents a D2 diagram formatter that converts database schemas into D2 format.
// It handles the conversion of database schemas to D2 diagrams and their subsequent rendering.
// The formatter uses an embedded template for diagram generation and supports customization
// through various options for rendering and compilation.
type Target struct {
	template    *template.Template
	renderOpts  *d2svg.RenderOpts
	compileOpts *d2lib.CompileOptions
}

// TargetOpt is a function type that allows customization of a Target instance.
// It is used to configure various aspects of the D2 diagram generation process.
type TargetOpt func(*Target)

// WithRenderOpts returns a TargetOpt that sets the rendering options for the D2 diagram.
// These options control aspects such as padding, theme, and other visual properties.
func WithRenderOpts(renderOpts *d2svg.RenderOpts) TargetOpt {
	return func(t *Target) {
		t.renderOpts = renderOpts
	}
}

// WithCompileOpts returns a TargetOpt that sets the compilation options for the D2 diagram.
// These options control the layout and measurement aspects of the diagram generation.
func WithCompileOpts(compileOpts *d2lib.CompileOptions) TargetOpt {
	return func(t *Target) {
		t.compileOpts = compileOpts
	}
}

// NewTarget creates a new D2 diagram formatter instance.
// It initializes the template from the embedded schema.tmpl file and sets up default
// rendering and compilation options. The formatter uses the ELK layout engine for
// diagram arrangement.
func NewTarget() (*Target, error) {
	tmpl, err := template.ParseFS(templateFS, "schema.tmpl")
	if err != nil {
		return nil, fmt.Errorf("parsing template: %w", err)
	}

	ruler, err := textmeasure.NewRuler()
	if err != nil {
		return nil, fmt.Errorf("creating ruler: %w", err)
	}

	layoutResolver := func(_ string) (d2graph.LayoutGraph, error) {
		return d2elklayout.DefaultLayout, nil
	}

	return &Target{
		template: tmpl,
		renderOpts: &d2svg.RenderOpts{
			Pad:     go2.Pointer(int64(5)),
			ThemeID: &d2themescatalog.Terminal.ID,
		},
		compileOpts: &d2lib.CompileOptions{
			LayoutResolver: layoutResolver,
			Ruler:          ruler,
		},
	}, nil
}

// Capabilities returns target capabilities.
func (t *Target) Capabilities() dberd.TargetCapabilities {
	return dberd.TargetCapabilities{
		Format: true,
		Render: true,
	}
}

// FormatSchema converts a database schema into D2 diagram format.
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

// RenderSchema renders a formatted D2 diagram to SVG format.
func (t *Target) RenderSchema(ctx context.Context, s dberd.FormattedSchema) ([]byte, error) {
	if s.Type != targetType {
		return nil, dberd.NewUnsupportedFormatError(s.Type, targetType)
	}

	ctx = log.WithDefault(ctx)

	diagram, _, err := d2lib.Compile(ctx, string(s.Data), t.compileOpts, t.renderOpts)
	if err != nil {
		return nil, fmt.Errorf("compiling diagram: %w", err)
	}

	out, err := d2svg.Render(diagram, t.renderOpts)
	if err != nil {
		return nil, fmt.Errorf("rendering diagram: %w", err)
	}

	return out, nil
}
