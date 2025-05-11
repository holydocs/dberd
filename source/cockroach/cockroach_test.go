package cockroach

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
	"github.com/testcontainers/testcontainers-go/modules/cockroachdb"
)

func TestExtractScheme(t *testing.T) {
	t.Parallel()

	container, db := setupTestDB(t)
	defer func() {
		err := container.Terminate(context.Background())
		if err != nil {
			slog.Warn("terminating cockroachdb container", "error", err)
		}
	}()
	defer db.Close()

	ctx := context.Background()

	// Create test schema
	_, err := db.ExecContext(ctx, `
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
	`)
	require.NoError(t, err)

	// Create source and extract schema
	source := NewSourceFromDB(db)
	actual, err := source.ExtractScheme(ctx)
	require.NoError(t, err)

	expected := dberd.Scheme{
		Tables: []dberd.Table{
			{
				Name: "public.users",
				Columns: []dberd.Column{
					{Name: "id", Definition: "INT8 NOT NULL", IsPrimary: true},
					{Name: "name", Definition: "VARCHAR(255) NOT NULL"},
					{Name: "email", Definition: "VARCHAR(255) NOT NULL", Comment: "User email address"},
					{Name: "created_at", Definition: "TIMESTAMP DEFAULT current_timestamp()"},
				},
			},
			{
				Name: "public.roles",
				Columns: []dberd.Column{
					{Name: "id", Definition: "INT8 NOT NULL", IsPrimary: true},
					{Name: "name", Definition: "VARCHAR(50) NOT NULL"},
					{Name: "description", Definition: "STRING", Comment: "Role description and permissions"},
					{Name: "created_at", Definition: "TIMESTAMP DEFAULT current_timestamp()"},
				},
			},
			{
				Name: "public.user_roles",
				Columns: []dberd.Column{
					{Name: "user_id", Definition: "INT8 NOT NULL", IsPrimary: true},
					{Name: "role_id", Definition: "INT8 NOT NULL", IsPrimary: true},
					{Name: "assigned_at", Definition: "TIMESTAMP DEFAULT current_timestamp()"},
				},
			},
			{
				Name: "public.posts",
				Columns: []dberd.Column{
					{Name: "id", Definition: "INT8 NOT NULL", IsPrimary: true},
					{Name: "user_id", Definition: "INT8 NOT NULL"},
					{Name: "title", Definition: "VARCHAR(255) NOT NULL"},
					{Name: "content", Definition: "STRING"},
					{Name: "created_at", Definition: "TIMESTAMP DEFAULT current_timestamp()"},
				},
			},
			{
				Name: "public.categories",
				Columns: []dberd.Column{
					{Name: "id", Definition: "INT8 NOT NULL", IsPrimary: true},
					{Name: "name", Definition: "VARCHAR(100) NOT NULL"},
					{Name: "description", Definition: "STRING"},
					{Name: "parent_id", Definition: "INT8", Comment: "Self-referencing foreign key for category hierarchy"},
					{Name: "created_at", Definition: "TIMESTAMP DEFAULT current_timestamp()"},
				},
			},
			{
				Name: "public.post_categories",
				Columns: []dberd.Column{
					{Name: "post_id", Definition: "INT8 NOT NULL", IsPrimary: true},
					{Name: "category_id", Definition: "INT8 NOT NULL", IsPrimary: true},
				},
			},
			{
				Name: "public.comments",
				Columns: []dberd.Column{
					{Name: "id", Definition: "INT8 NOT NULL", IsPrimary: true},
					{Name: "post_id", Definition: "INT8 NOT NULL"},
					{Name: "user_id", Definition: "INT8 NOT NULL"},
					{Name: "content", Definition: "STRING NOT NULL"},
					{Name: "created_at", Definition: "TIMESTAMP DEFAULT current_timestamp()"},
				},
			},
		},
		References: []dberd.Reference{
			{Source: dberd.TableColumn{Table: "public.categories", Column: "parent_id"}, Target: dberd.TableColumn{Table: "public.categories", Column: "id"}},
			{Source: dberd.TableColumn{Table: "public.comments", Column: "post_id"}, Target: dberd.TableColumn{Table: "public.posts", Column: "id"}},
			{Source: dberd.TableColumn{Table: "public.comments", Column: "user_id"}, Target: dberd.TableColumn{Table: "public.users", Column: "id"}},
			{Source: dberd.TableColumn{Table: "public.post_categories", Column: "category_id"}, Target: dberd.TableColumn{Table: "public.categories", Column: "id"}},
			{Source: dberd.TableColumn{Table: "public.post_categories", Column: "post_id"}, Target: dberd.TableColumn{Table: "public.posts", Column: "id"}},
			{Source: dberd.TableColumn{Table: "public.posts", Column: "user_id"}, Target: dberd.TableColumn{Table: "public.users", Column: "id"}},
			{Source: dberd.TableColumn{Table: "public.user_roles", Column: "role_id"}, Target: dberd.TableColumn{Table: "public.roles", Column: "id"}},
			{Source: dberd.TableColumn{Table: "public.user_roles", Column: "user_id"}, Target: dberd.TableColumn{Table: "public.users", Column: "id"}},
		},
	}
	for _, s := range []dberd.Scheme{actual, expected} {
		sort.Slice(s.Tables, func(i, j int) bool {
			return s.Tables[i].Name < s.Tables[j].Name
		})
		sort.Slice(s.References, func(i, j int) bool {
			switch {
			case s.References[i].Source.Table != s.References[j].Source.Table:
				return s.References[i].Source.Table < s.References[j].Source.Table
			case s.References[i].Source.Column != s.References[j].Source.Column:
				return s.References[i].Source.Column < s.References[j].Source.Column
			case s.References[i].Target.Table != s.References[j].Target.Table:
				return s.References[i].Target.Table < s.References[j].Target.Table
			default:
				return s.References[i].Target.Column < s.References[j].Target.Column
			}
		})
	}
	assert.Equal(t, expected, actual)
}

func setupTestDB(t *testing.T) (testcontainers.Container, *sql.DB) {
	ctx := context.Background()

	container, err := cockroachdb.RunContainer(ctx,
		testcontainers.WithImage("cockroachdb/cockroach:latest-v23.1"),
	)
	require.NoError(t, err)

	connStr, err := container.ConnectionString(ctx)
	require.NoError(t, err)

	db, err := sql.Open("pgx", connStr)
	require.NoError(t, err)

	// Wait for the database to be ready
	require.Eventually(t, func() bool {
		err := db.PingContext(ctx)
		return err == nil
	}, 10*time.Second, 100*time.Millisecond)

	return container, db
}
