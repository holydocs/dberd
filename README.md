# DBerd

[![Run Tests](https://github.com/denchenko/dberd/actions/workflows/go.yml/badge.svg?branch=main)](https://github.com/denchenko/dberd/actions/workflows/go.yml)
[![codecov](https://codecov.io/gh/denchenko/dberd/branch/main/graph/badge.svg)](https://codecov.io/gh/denchenko/dberd)
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
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/denchenko/dberd/source/cockroach"
	"github.com/denchenko/dberd/target/d2"
)

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

	err = os.WriteFile("out.svg", diagram, 0600)
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

For example, if a Cockroach database has a schema like:
```
CREATE TABLE users (
	id INT PRIMARY KEY,
	name VARCHAR(255) NOT NULL,
	email VARCHAR(255) NOT NULL,
	created_at TIMESTAMP DEFAULT current_timestamp()
);

CREATE TABLE roles (
	id INT PRIMARY KEY,
	name VARCHAR(50) NOT NULL,
	description STRING,
	created_at TIMESTAMP DEFAULT current_timestamp()
);

CREATE TABLE user_roles (
	user_id INT NOT NULL,
	role_id INT NOT NULL,
	assigned_at TIMESTAMP DEFAULT current_timestamp(),
	PRIMARY KEY (user_id, role_id),
	FOREIGN KEY (user_id) REFERENCES users(id),
	FOREIGN KEY (role_id) REFERENCES roles(id)
);

CREATE TABLE posts (
	id INT PRIMARY KEY,
	user_id INT NOT NULL,
	title VARCHAR(255) NOT NULL,
	content STRING,
	created_at TIMESTAMP DEFAULT current_timestamp(),
	FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE categories (
	id INT PRIMARY KEY,
	name VARCHAR(100) NOT NULL,
	description STRING,
	parent_id INT,
	created_at TIMESTAMP DEFAULT current_timestamp(),
	FOREIGN KEY (parent_id) REFERENCES categories(id)
);

CREATE TABLE post_categories (
	post_id INT NOT NULL,
	category_id INT NOT NULL,
	PRIMARY KEY (post_id, category_id),
	FOREIGN KEY (post_id) REFERENCES posts(id),
	FOREIGN KEY (category_id) REFERENCES categories(id)
);

CREATE TABLE comments (
	id INT PRIMARY KEY,
	post_id INT NOT NULL,
	user_id INT NOT NULL,
	content STRING NOT NULL,
	created_at TIMESTAMP DEFAULT current_timestamp(),
	FOREIGN KEY (post_id) REFERENCES posts(id),
	FOREIGN KEY (user_id) REFERENCES users(id)
);

COMMENT ON COLUMN users.email IS 'User email address';
COMMENT ON COLUMN roles.description IS 'Role description and permissions';
COMMENT ON COLUMN categories.parent_id IS 'Self-referencing foreign key for category hierarchy';
```

The resulting `scheme.d2` will be:
```
direction: right

# Tables
public.users: {
  shape: "sql_table"
  id: "INT8 NOT NULL" { constraint: [primary_key] }
  name: "VARCHAR(255) NOT NULL"
  email: "VARCHAR(255) NOT NULL"
  created_at: "TIMESTAMP DEFAULT current_timestamp()"
}
public.roles: {
  shape: "sql_table"
  id: "INT8 NOT NULL" { constraint: [primary_key] }
  name: "VARCHAR(50) NOT NULL"
  description: "STRING"
  created_at: "TIMESTAMP DEFAULT current_timestamp()"
}
public.user_roles: {
  shape: "sql_table"
  user_id: "INT8 NOT NULL" { constraint: [primary_key] }
  role_id: "INT8 NOT NULL" { constraint: [primary_key] }
  assigned_at: "TIMESTAMP DEFAULT current_timestamp()"
}
public.posts: {
  shape: "sql_table"
  id: "INT8 NOT NULL" { constraint: [primary_key] }
  user_id: "INT8 NOT NULL"
  title: "VARCHAR(255) NOT NULL"
  content: "STRING"
  created_at: "TIMESTAMP DEFAULT current_timestamp()"
}
public.categories: {
  shape: "sql_table"
  id: "INT8 NOT NULL" { constraint: [primary_key] }
  name: "VARCHAR(100) NOT NULL"
  description: "STRING"
  parent_id: "INT8"
  created_at: "TIMESTAMP DEFAULT current_timestamp()"
}
public.post_categories: {
  shape: "sql_table"
  post_id: "INT8 NOT NULL" { constraint: [primary_key] }
  category_id: "INT8 NOT NULL" { constraint: [primary_key] }
}
public.comments: {
  shape: "sql_table"
  id: "INT8 NOT NULL" { constraint: [primary_key] }
  post_id: "INT8 NOT NULL"
  user_id: "INT8 NOT NULL"
  content: "STRING NOT NULL"
  created_at: "TIMESTAMP DEFAULT current_timestamp()"
}

# References
public.categories.parent_id -> public.categories.id
public.comments.post_id -> public.posts.id
public.comments.user_id -> public.users.id
public.post_categories.category_id -> public.categories.id
public.post_categories.post_id -> public.posts.id
public.posts.user_id -> public.users.id
public.user_roles.role_id -> public.roles.id
public.user_roles.user_id -> public.users.id
```

And the resulting `scheme.svg` will be:
![scheme](target/d2/testdata/scheme.svg)
