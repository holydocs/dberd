package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/denchenko/dberd"
	"github.com/denchenko/dberd/source/cockroach"
	"github.com/denchenko/dberd/target/d2"
	"github.com/denchenko/dberd/target/json"
	"github.com/denchenko/dberd/target/plantuml"
)

func main() {
	sourceType := flag.String("source", "", "Source database type (e.g., cockroach)")
	targetType := flag.String("target", "", "Target type (e.g., d2)")
	formatToFile := flag.String("format-to-file", "", "Output file for the formatted schema")
	renderToFile := flag.String("render-to-file", "", "Output file for the rendered diagram")
	sourceDSN := flag.String("source-dsn", "", "Connection string for source database")

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

	scheme, err := source.ExtractScheme(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Extracting scheme %v\n", err)
		os.Exit(1)
	}

	fs, err := target.FormatScheme(ctx, scheme)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Formatting scheme %v\n", err)
		os.Exit(1)
	}

	if *formatToFile != "" {
		err = os.WriteFile(*formatToFile, fs.Data, 0600)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error : Writing to file %v\n", err)
			os.Exit(1)
		}
	}

	if *renderToFile != "" {
		diagram, err := target.RenderScheme(ctx, fs)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: Rendering scheme %v\n", err)
			os.Exit(1)
		}

		err = os.WriteFile(*renderToFile, diagram, 0600)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: Writing to file %v\n", err)
			os.Exit(1)
		}
	}
}

func pickSource(sourceType, sourceDSN string) (dberd.Source, error) {
	switch sourceType {
	case "cockroach":
		return cockroach.NewSource(sourceDSN)
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
	}
	return nil, errors.New("unknown target")
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: dberd [options]\n\n")
	fmt.Fprintf(os.Stderr, "Options:\n")
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\nExample:\n")
	fmt.Fprintf(os.Stderr, "  dberd --source cockroach --target d2 --format-to-file scheme.d2 --render-to-file scheme.svg --source-dsn \"connection-string\"\n")
}
