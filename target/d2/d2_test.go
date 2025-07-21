package d2

import (
	"context"
	_ "embed"
	"testing"

	"github.com/holydocs/dberd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	//go:embed testdata/schema.d2
	testSchema []byte

	//go:embed testdata/schema.svg
	testSVG []byte
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

	ctx := context.Background()

	target, err := NewTarget()
	require.NoError(t, err)

	actual, err := target.FormatSchema(ctx, schema)
	require.NoError(t, err)

	expected := dberd.FormattedSchema{
		Type: "d2",
		Data: testSchema,
	}
	assert.Equal(t, expected, actual)
}

func TestRenderSchema(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	target, err := NewTarget()
	require.NoError(t, err)

	actual, err := target.RenderSchema(ctx, dberd.FormattedSchema{
		Type: "d2",
		Data: testSchema,
	})
	require.NoError(t, err)

	assert.Equal(t, testSVG, actual)
}
