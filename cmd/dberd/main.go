package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/holydocs/dberd"
	"github.com/holydocs/dberd/source/clickhouse"
	"github.com/holydocs/dberd/source/cockroach"
	"github.com/holydocs/dberd/source/mongodb"
	"github.com/holydocs/dberd/source/mysql"
	"github.com/holydocs/dberd/source/postgres"
	"github.com/holydocs/dberd/target/d2"
	"github.com/holydocs/dberd/target/json"
	"github.com/holydocs/dberd/target/mermaid"
	"github.com/holydocs/dberd/target/plantuml"
)

func main() {
	sourceType := flag.String("source", "", "Source database type (postgres, mysql, cockroach, clickhouse, mongodb)")
	targetType := flag.String("target", "", "Target type (d2, plantuml, json, mermaid)")
	formatToFile := flag.String("format-to-file", "", "Output file for the formatted schema")
	renderToFile := flag.String("render-to-file", "", "Output file for the rendered diagram")
	sourceDSN := flag.String("source-dsn", "", "Connection string for source database")
	skipTables := flag.String("skip-tables", "", "Comma-separated table names to omit from the output schema")
	addLogicalReferences := flag.Bool("add-logical-references", false, "Infer references from <table>_id column names")
	logicalReferencesFile := flag.String("logical-references-file", "", "YAML file containing predefined logical references")

	help := flag.Bool("help", false, "Show help")

	flag.Parse()

	if *help ||
		*sourceType == "" ||
		*targetType == "" ||
		(*formatToFile == "" && *renderToFile == "") {
		printUsage()
		os.Exit(1)
	}

	source, err := pickSource(*sourceType, *sourceDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	defer source.Close()

	target, err := pickTarget(*targetType)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	targetCaps := target.Capabilities()

	if !targetCaps.Format {
		fmt.Fprintf(os.Stderr, "Error: Target doesn't support formatting\n")
		os.Exit(1)
	}

	if *renderToFile != "" && !targetCaps.Render {
		fmt.Fprintf(os.Stderr, "Error: Target doesn't support render\n")
		os.Exit(1)
	}

	ctx := context.Background()

	schema, err := source.ExtractSchema(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Extracting schema %v\n", err)
		os.Exit(1)
	}

	schema.SkipTables(strings.Split(*skipTables, ",")...)
	if *logicalReferencesFile != "" {
		predefinedReferences, err := loadPredefinedLogicalReferences(*logicalReferencesFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: Loading predefined logical references %v\n", err)
			os.Exit(1)
		}

		if err := schema.AddPredefinedLogicalReferences(predefinedReferences); err != nil {
			fmt.Fprintf(os.Stderr, "Error: Adding predefined logical references %v\n", err)
			os.Exit(1)
		}
	}
	if *addLogicalReferences {
		schema.AddLogicalReferences()
	}
	schema.Sort()

	fs, err := target.FormatSchema(ctx, schema)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Formatting schema %v\n", err)
		os.Exit(1)
	}

	if *formatToFile != "" {
		err = os.WriteFile(*formatToFile, fs.Data, 0o600)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error : Writing to file %v\n", err)
			os.Exit(1)
		}
	}

	if *renderToFile != "" {
		diagram, err := target.RenderSchema(ctx, fs)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: Rendering schema %v\n", err)
			os.Exit(1)
		}

		err = os.WriteFile(*renderToFile, diagram, 0o600)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: Writing to file %v\n", err)
			os.Exit(1)
		}
	}
}

func loadPredefinedLogicalReferences(path string) ([]dberd.Reference, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", path, err)
	}
	defer file.Close()

	references, err := dberd.ParsePredefinedLogicalReferences(file)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}

	return references, nil
}

func pickSource(sourceType, sourceDSN string) (dberd.Source, error) {
	switch sourceType {
	case "postgres":
		return postgres.NewSource(sourceDSN)
	case "mysql":
		return mysql.NewSource(sourceDSN)
	case "cockroach":
		return cockroach.NewSource(sourceDSN)
	case "clickhouse":
		return clickhouse.NewSource(sourceDSN)
	case "mongodb":
		return mongodb.NewSource(sourceDSN)
	}
	return nil, errors.New("unknown source")
}

func pickTarget(targetType string) (dberd.Target, error) {
	switch targetType {
	case "d2":
		return d2.NewTarget()
	case "plantuml":
		return plantuml.NewTarget()
	case "json":
		return json.NewTarget(), nil
	case "mermaid":
		return mermaid.NewTarget()
	}
	return nil, errors.New("unknown target")
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: dberd [options]\n\n")
	fmt.Fprintf(os.Stderr, "Options:\n")
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\nExample:\n")
	fmt.Fprintf(os.Stderr, "  dberd --source cockroach --target d2 --format-to-file schema.d2 --render-to-file schema.svg --source-dsn \"connection-string\"\n")
}
