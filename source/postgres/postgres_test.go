package postgres

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/holydocs/dberd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestExtractSchema(t *testing.T) {
	t.Parallel()

	container, db := setupTestDB(t)
	defer func() {
		err := container.Terminate(context.Background())
		if err != nil {
			slog.Warn("terminating postgres container", "error", err)
		}
	}()
	defer db.Close()

	ctx := context.Background()

	// Create test schema
	_, err := db.ExecContext(ctx, `
		CREATE SCHEMA IF NOT EXISTS public;

		CREATE TABLE public.users (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			email VARCHAR(255) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE public.roles (
			id SERIAL PRIMARY KEY,
			name VARCHAR(50) NOT NULL,
			description TEXT,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE public.user_roles (
			user_id INTEGER NOT NULL,
			role_id INTEGER NOT NULL,
			assigned_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (user_id, role_id),
			FOREIGN KEY (user_id) REFERENCES users(id),
			FOREIGN KEY (role_id) REFERENCES roles(id)
		);

		CREATE TABLE public.posts (
			id SERIAL PRIMARY KEY,
			user_id INTEGER NOT NULL,
			title VARCHAR(255) NOT NULL,
			content TEXT,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (user_id) REFERENCES users(id)
		);

		CREATE TABLE public.categories (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			description TEXT,
			parent_id INTEGER,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (parent_id) REFERENCES categories(id)
		);

		CREATE TABLE public.post_categories (
			post_id INTEGER NOT NULL,
			category_id INTEGER NOT NULL,
			PRIMARY KEY (post_id, category_id),
			FOREIGN KEY (post_id) REFERENCES posts(id),
			FOREIGN KEY (category_id) REFERENCES categories(id)
		);

		CREATE TABLE public.comments (
			id SERIAL PRIMARY KEY,
			post_id INTEGER NOT NULL,
			user_id INTEGER NOT NULL,
			content TEXT NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (post_id) REFERENCES posts(id),
			FOREIGN KEY (user_id) REFERENCES users(id)
		);

		COMMENT ON COLUMN public.users.email IS 'User email address';
		COMMENT ON COLUMN public.roles.description IS 'Role description and permissions';
		COMMENT ON COLUMN public.categories.parent_id IS 'Self-referencing foreign key for category hierarchy';
	`)
	require.NoError(t, err)

	source := NewSourceFromDB(db)

	actual, err := source.ExtractSchema(ctx)
	require.NoError(t, err)

	actual.Sort()

	expected := dberd.Schema{
		Tables: []dberd.Table{
			{
				Name: "public.users",
				Columns: []dberd.Column{
					{Name: "id", Definition: "INTEGER NOT NULL DEFAULT nextval('users_id_seq'::regclass)", IsPrimary: true},
					{Name: "name", Definition: "CHARACTER VARYING NOT NULL"},
					{Name: "email", Definition: "CHARACTER VARYING NOT NULL", Comment: "User email address"},
					{Name: "created_at", Definition: "TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP"},
				},
			},
			{
				Name: "public.roles",
				Columns: []dberd.Column{
					{Name: "id", Definition: "INTEGER NOT NULL DEFAULT nextval('roles_id_seq'::regclass)", IsPrimary: true},
					{Name: "name", Definition: "CHARACTER VARYING NOT NULL"},
					{Name: "description", Definition: "TEXT", Comment: "Role description and permissions"},
					{Name: "created_at", Definition: "TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP"},
				},
			},
			{
				Name: "public.user_roles",
				Columns: []dberd.Column{
					{Name: "user_id", Definition: "INTEGER NOT NULL", IsPrimary: true},
					{Name: "role_id", Definition: "INTEGER NOT NULL", IsPrimary: true},
					{Name: "assigned_at", Definition: "TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP"},
				},
			},
			{
				Name: "public.posts",
				Columns: []dberd.Column{
					{Name: "id", Definition: "INTEGER NOT NULL DEFAULT nextval('posts_id_seq'::regclass)", IsPrimary: true},
					{Name: "user_id", Definition: "INTEGER NOT NULL"},
					{Name: "title", Definition: "CHARACTER VARYING NOT NULL"},
					{Name: "content", Definition: "TEXT"},
					{Name: "created_at", Definition: "TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP"},
				},
			},
			{
				Name: "public.categories",
				Columns: []dberd.Column{
					{Name: "id", Definition: "INTEGER NOT NULL DEFAULT nextval('categories_id_seq'::regclass)", IsPrimary: true},
					{Name: "name", Definition: "CHARACTER VARYING NOT NULL"},
					{Name: "description", Definition: "TEXT"},
					{Name: "parent_id", Definition: "INTEGER", Comment: "Self-referencing foreign key for category hierarchy"},
					{Name: "created_at", Definition: "TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP"},
				},
			},
			{
				Name: "public.post_categories",
				Columns: []dberd.Column{
					{Name: "post_id", Definition: "INTEGER NOT NULL", IsPrimary: true},
					{Name: "category_id", Definition: "INTEGER NOT NULL", IsPrimary: true},
				},
			},
			{
				Name: "public.comments",
				Columns: []dberd.Column{
					{Name: "id", Definition: "INTEGER NOT NULL DEFAULT nextval('comments_id_seq'::regclass)", IsPrimary: true},
					{Name: "post_id", Definition: "INTEGER NOT NULL"},
					{Name: "user_id", Definition: "INTEGER NOT NULL"},
					{Name: "content", Definition: "TEXT NOT NULL"},
					{Name: "created_at", Definition: "TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP"},
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

	expected.Sort()

	assert.Equal(t, expected, actual)
}

func setupTestDB(t *testing.T) (testcontainers.Container, *sql.DB) {
	ctx := context.Background()

	container, err := postgres.Run(ctx,
		"postgres:15-alpine",
		postgres.WithDatabase("test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	require.NoError(t, err)

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	db, err := sql.Open("pgx", connStr)
	require.NoError(t, err)

	return container, db
}
