# DBerd

[![Run Tests](https://github.com/denchenko/dberd/actions/workflows/go.yml/badge.svg?branch=master)](https://github.com/denchenko/dberd/actions/workflows/go.yml)
[![codecov](https://codecov.io/gh/denchenko/dberd/branch/master/graph/badge.svg)](https://codecov.io/gh/denchenko/dberd)
[![Go Report Card](https://goreportcard.com/badge/github.com/denchenko/dberd)](https://goreportcard.com/report/github.com/denchenko/dberd)
[![GoDoc](https://godoc.org/github.com/denchenko/dberd?status.svg)](https://godoc.org/github.com/denchenko/dberd)

DBerd is a Go library and command-line tool that helps you extract database schemas and generate diagrams. It provides a flexible and extensible way to work with different database sources and output formats.

## Features

- Extract database schemas from various sources
- Format schemas into different representations
- Generate diagrams using multiple diagramming tools
- Use as a library in your Go applications or as a standalone CLI tool
- Extensible architecture for adding new sources and targets

## Installation

### As a Library

```bash
go get github.com/denchenko/dberd
```

### As a CLI Tool

```bash
# Using Go
go install github.com/denchenko/dberd/cmd/dberd@latest
```

## Supported Sources

Currently, DBerd supports the following database sources:

- **CockroachDB**: Extract schema from CockroachDB databases using the `cockroach` source type.

## Supported Targets

DBerd supports multiple output formats and diagramming tools:

- **D2**: Generate diagrams using the D2 diagramming language
- **PlantUML**: Generate diagrams using PlantUML
- **JSON**: Output schema in JSON format

## Usage

### As a Library

Here's a simple example of how to use DBerd as a library to extract a schema from CockroachDB and generate a D2 diagram:

```go
func main() {
	cockroachSource, err := cockroach.NewSource("postgres://user@host:port/db?sslmode=disable")
	if err != nil {
		panic(fmt.Errorf("creating new cockroach source: %w", err))
	}
	defer cockroachSource.Close()

	ctx := context.Background()

	scheme, err := cockroachSource.ExtractScheme(ctx)
	if err != nil {
		panic(fmt.Errorf("extracting cockroach scheme: %w", err))
	}

	d2Target, err := d2.NewTarget()
	if err != nil {
		panic(fmt.Errorf("creating d2 target: %w", err))
	}

	formattedScheme, err := d2Target.FormatScheme(ctx, scheme)
	if err != nil {
		panic(fmt.Errorf("formatting cockroach scheme into d2: %w", err))
	}

	diagram, err := d2Target.RenderScheme(ctx, formattedScheme)
	if err != nil {
		panic(fmt.Errorf("rendering cockroach scheme into d2: %w", err))
	}

	err = os.WriteFile(filepath.Join("out.svg"), diagram, 0600)
	if err != nil {
		panic(fmt.Errorf("writing file: %w", err))
	}
}
```

### As a CLI Tool

DBerd can be used as a command-line tool to extract and visualize database schemas:

```bash
# Extract schema and generate D2 diagram
dberd --source cockroach \
      --target d2 \
      --format-to-file scheme.d2 \
      --render-to-file scheme.svg \
      --source-dsn "postgres://user@host:port/db?sslmode=disable"
```
