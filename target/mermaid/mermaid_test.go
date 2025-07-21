package mermaid

import (
	"context"
	_ "embed"
	"testing"

	"github.com/holydocs/dberd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	//go:embed testdata/schema.mmd
	testSchema []byte
)

func TestFormatSchema(t *testing.T) {
	t.Parallel()

	schema := dberd.Schema{
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
		},
		References: []dberd.Reference{
			{Source: dberd.TableColumn{Table: "public.user_roles", Column: "role_id"}, Target: dberd.TableColumn{Table: "public.roles", Column: "id"}},
			{Source: dberd.TableColumn{Table: "public.user_roles", Column: "user_id"}, Target: dberd.TableColumn{Table: "public.users", Column: "id"}},
			{Source: dberd.TableColumn{Table: "public.posts", Column: "user_id"}, Target: dberd.TableColumn{Table: "public.users", Column: "id"}},
		},
	}

	ctx := context.Background()

	target, err := NewTarget()
	require.NoError(t, err)

	actual, err := target.FormatSchema(ctx, schema)
	require.NoError(t, err)

	expected := dberd.FormattedSchema{
		Type: targetType,
		Data: testSchema,
	}

	assert.Equal(t, expected, actual)
}
