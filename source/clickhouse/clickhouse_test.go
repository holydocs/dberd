package clickhouse

import (
	"context"
	"database/sql"
	"log/slog"
	"sort"
	"testing"
	"time"

	"github.com/denchenko/dberd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/clickhouse"
)

func TestExtractSchema(t *testing.T) {
	t.Parallel()

	container, db := setupTestDB(t)
	defer func() {
		err := container.Terminate(context.Background())
		if err != nil {
			slog.Warn("terminating clickhouse container", "error", err)
		}
	}()
	defer db.Close()

	ctx := context.Background()

	// Create test schema
	statements := []string{
		`CREATE TABLE users (
			id UInt32,
			name String,
			email String,
			created_at DateTime DEFAULT now(),
			PRIMARY KEY (id)
		) ENGINE = MergeTree();`,
		`CREATE TABLE roles (
			id UInt32,
			name String,
			description String,
			created_at DateTime DEFAULT now(),
			PRIMARY KEY (id)
		) ENGINE = MergeTree();`,
		`CREATE TABLE user_roles (
			user_id UInt32,
			role_id UInt32,
			assigned_at DateTime DEFAULT now(),
			PRIMARY KEY (user_id, role_id)
		) ENGINE = MergeTree();`,
		`ALTER TABLE users COMMENT COLUMN email 'User email address';`,
		`ALTER TABLE roles COMMENT COLUMN description 'Role description and permissions';`,
	}
	for _, statement := range statements {
		_, err := db.ExecContext(ctx, statement)
		require.NoError(t, err)
	}

	source := NewSourceFromDB(db)
	actual, err := source.ExtractSchema(ctx)
	require.NoError(t, err)

	expected := dberd.Schema{
		Tables: []dberd.Table{
			{
				Name: "clickhouse.users",
				Columns: []dberd.Column{
					{Name: "id", Definition: "UInt32", IsPrimary: true},
					{Name: "name", Definition: "String"},
					{Name: "email", Definition: "String", Comment: "User email address"},
					{Name: "created_at", Definition: "DateTime DEFAULT now()"},
				},
			},
			{
				Name: "clickhouse.roles",
				Columns: []dberd.Column{
					{Name: "id", Definition: "UInt32", IsPrimary: true},
					{Name: "name", Definition: "String"},
					{Name: "description", Definition: "String", Comment: "Role description and permissions"},
					{Name: "created_at", Definition: "DateTime DEFAULT now()"},
				},
			},
			{
				Name: "clickhouse.user_roles",
				Columns: []dberd.Column{
					{Name: "user_id", Definition: "UInt32", IsPrimary: true},
					{Name: "role_id", Definition: "UInt32", IsPrimary: true},
					{Name: "assigned_at", Definition: "DateTime DEFAULT now()"},
				},
			},
		},
	}

	// Sort tables and columns for consistent comparison
	for _, s := range []dberd.Schema{actual, expected} {
		sort.Slice(s.Tables, func(i, j int) bool {
			return s.Tables[i].Name < s.Tables[j].Name
		})
		for _, table := range s.Tables {
			sort.Slice(table.Columns, func(i, j int) bool {
				return table.Columns[i].Name < table.Columns[j].Name
			})
		}
	}

	assert.Equal(t, expected, actual)
}

func setupTestDB(t *testing.T) (testcontainers.Container, *sql.DB) {
	ctx := context.Background()

	container, err := clickhouse.Run(ctx, "clickhouse/clickhouse-server:latest")
	require.NoError(t, err)

	connStr, err := container.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("clickhouse", connStr)
	require.NoError(t, err)

	// Wait for the database to be ready
	require.Eventually(t, func() bool {
		err := db.PingContext(ctx)
		return err == nil
	}, 10*time.Second, 100*time.Millisecond)

	return container, db
}
