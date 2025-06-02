package mysql

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/denchenko/dberd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
)

func TestExtractSchema(t *testing.T) {
	t.Parallel()

	container, db := setupTestDB(t)
	defer func() {
		err := container.Terminate(context.Background())
		if err != nil {
			slog.Warn("terminating mysql container", "error", err)
		}
	}()
	defer db.Close()

	ctx := context.Background()

	// Create test schema
	_, err := db.ExecContext(ctx, `
		CREATE TABLE users (
			id INT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			email VARCHAR(255) NOT NULL COMMENT 'User email address',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `
		CREATE TABLE roles (
			id INT PRIMARY KEY,
			name VARCHAR(50) NOT NULL,
			description TEXT COMMENT 'Role description and permissions',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `
		CREATE TABLE user_roles (
			user_id INT NOT NULL,
			role_id INT NOT NULL,
			assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (user_id, role_id),
			FOREIGN KEY (user_id) REFERENCES users(id),
			FOREIGN KEY (role_id) REFERENCES roles(id)
		)`)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `
		CREATE TABLE posts (
			id INT PRIMARY KEY,
			user_id INT NOT NULL,
			title VARCHAR(255) NOT NULL,
			content TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (user_id) REFERENCES users(id)
		)`)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `
		CREATE TABLE categories (
			id INT PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			description TEXT,
			parent_id INT COMMENT 'Self-referencing foreign key for category hierarchy',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (parent_id) REFERENCES categories(id)
		)`)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `
		CREATE TABLE post_categories (
			post_id INT NOT NULL,
			category_id INT NOT NULL,
			PRIMARY KEY (post_id, category_id),
			FOREIGN KEY (post_id) REFERENCES posts(id),
			FOREIGN KEY (category_id) REFERENCES categories(id)
		)`)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `
		CREATE TABLE comments (
			id INT PRIMARY KEY,
			post_id INT NOT NULL,
			user_id INT NOT NULL,
			content TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (post_id) REFERENCES posts(id),
			FOREIGN KEY (user_id) REFERENCES users(id)
		)`)
	require.NoError(t, err)

	source := NewSourceFromDB(db)

	actual, err := source.ExtractSchema(ctx)
	require.NoError(t, err)

	actual.Sort()

	expected := dberd.Schema{
		Tables: []dberd.Table{
			{
				Name: "test.users",
				Columns: []dberd.Column{
					{Name: "id", Definition: "int NOT NULL", IsPrimary: true},
					{Name: "name", Definition: "varchar(255) NOT NULL"},
					{Name: "email", Definition: "varchar(255) NOT NULL", Comment: "User email address"},
					{Name: "created_at", Definition: "timestamp DEFAULT CURRENT_TIMESTAMP"},
				},
			},
			{
				Name: "test.roles",
				Columns: []dberd.Column{
					{Name: "id", Definition: "int NOT NULL", IsPrimary: true},
					{Name: "name", Definition: "varchar(50) NOT NULL"},
					{Name: "description", Definition: "text", Comment: "Role description and permissions"},
					{Name: "created_at", Definition: "timestamp DEFAULT CURRENT_TIMESTAMP"},
				},
			},
			{
				Name: "test.user_roles",
				Columns: []dberd.Column{
					{Name: "user_id", Definition: "int NOT NULL", IsPrimary: true},
					{Name: "role_id", Definition: "int NOT NULL", IsPrimary: true},
					{Name: "assigned_at", Definition: "timestamp DEFAULT CURRENT_TIMESTAMP"},
				},
			},
			{
				Name: "test.posts",
				Columns: []dberd.Column{
					{Name: "id", Definition: "int NOT NULL", IsPrimary: true},
					{Name: "user_id", Definition: "int NOT NULL"},
					{Name: "title", Definition: "varchar(255) NOT NULL"},
					{Name: "content", Definition: "text"},
					{Name: "created_at", Definition: "timestamp DEFAULT CURRENT_TIMESTAMP"},
				},
			},
			{
				Name: "test.categories",
				Columns: []dberd.Column{
					{Name: "id", Definition: "int NOT NULL", IsPrimary: true},
					{Name: "name", Definition: "varchar(100) NOT NULL"},
					{Name: "description", Definition: "text"},
					{Name: "parent_id", Definition: "int", Comment: "Self-referencing foreign key for category hierarchy"},
					{Name: "created_at", Definition: "timestamp DEFAULT CURRENT_TIMESTAMP"},
				},
			},
			{
				Name: "test.post_categories",
				Columns: []dberd.Column{
					{Name: "post_id", Definition: "int NOT NULL", IsPrimary: true},
					{Name: "category_id", Definition: "int NOT NULL", IsPrimary: true},
				},
			},
			{
				Name: "test.comments",
				Columns: []dberd.Column{
					{Name: "id", Definition: "int NOT NULL", IsPrimary: true},
					{Name: "post_id", Definition: "int NOT NULL"},
					{Name: "user_id", Definition: "int NOT NULL"},
					{Name: "content", Definition: "text NOT NULL"},
					{Name: "created_at", Definition: "timestamp DEFAULT CURRENT_TIMESTAMP"},
				},
			},
		},
		References: []dberd.Reference{
			{Source: dberd.TableColumn{Table: "test.categories", Column: "parent_id"}, Target: dberd.TableColumn{Table: "test.categories", Column: "id"}},
			{Source: dberd.TableColumn{Table: "test.comments", Column: "post_id"}, Target: dberd.TableColumn{Table: "test.posts", Column: "id"}},
			{Source: dberd.TableColumn{Table: "test.comments", Column: "user_id"}, Target: dberd.TableColumn{Table: "test.users", Column: "id"}},
			{Source: dberd.TableColumn{Table: "test.post_categories", Column: "category_id"}, Target: dberd.TableColumn{Table: "test.categories", Column: "id"}},
			{Source: dberd.TableColumn{Table: "test.post_categories", Column: "post_id"}, Target: dberd.TableColumn{Table: "test.posts", Column: "id"}},
			{Source: dberd.TableColumn{Table: "test.posts", Column: "user_id"}, Target: dberd.TableColumn{Table: "test.users", Column: "id"}},
			{Source: dberd.TableColumn{Table: "test.user_roles", Column: "role_id"}, Target: dberd.TableColumn{Table: "test.roles", Column: "id"}},
			{Source: dberd.TableColumn{Table: "test.user_roles", Column: "user_id"}, Target: dberd.TableColumn{Table: "test.users", Column: "id"}},
		},
	}

	expected.Sort()

	assert.Equal(t, expected, actual)
}

func setupTestDB(t *testing.T) (testcontainers.Container, *sql.DB) {
	ctx := context.Background()

	container, err := mysql.Run(ctx,
		"mysql:8.0",
		mysql.WithDatabase("test"),
		mysql.WithUsername("test"),
		mysql.WithPassword("test"),
	)
	require.NoError(t, err)

	connStr, err := container.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("mysql", connStr)
	require.NoError(t, err)

	// Wait for the database to be ready
	require.Eventually(t, func() bool {
		err := db.PingContext(ctx)
		return err == nil
	}, 10*time.Second, 100*time.Millisecond)

	return container, db
}
