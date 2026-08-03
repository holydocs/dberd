package dberd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSchema_Sort(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		schema   Schema
		expected Schema
	}{
		{
			name: "sorts tables and references",
			schema: Schema{
				Tables: []Table{
					{
						Name: "z_table",
						Columns: []Column{
							{Name: "z_column", Definition: "text"},
							{Name: "a_column", Definition: "text"},
						},
					},
					{
						Name: "a_table",
						Columns: []Column{
							{Name: "z_column", Definition: "text"},
							{Name: "a_column", Definition: "text"},
						},
					},
				},
				References: []Reference{
					{
						Source: TableColumn{Table: "z_table", Column: "z_column"},
						Target: TableColumn{Table: "a_table", Column: "a_column"},
					},
					{
						Source: TableColumn{Table: "a_table", Column: "a_column"},
						Target: TableColumn{Table: "z_table", Column: "z_column"},
					},
				},
			},
			expected: Schema{
				Tables: []Table{
					{
						Name: "a_table",
						Columns: []Column{
							{Name: "a_column", Definition: "text"},
							{Name: "z_column", Definition: "text"},
						},
					},
					{
						Name: "z_table",
						Columns: []Column{
							{Name: "a_column", Definition: "text"},
							{Name: "z_column", Definition: "text"},
						},
					},
				},
				References: []Reference{
					{
						Source: TableColumn{Table: "a_table", Column: "a_column"},
						Target: TableColumn{Table: "z_table", Column: "z_column"},
					},
					{
						Source: TableColumn{Table: "z_table", Column: "z_column"},
						Target: TableColumn{Table: "a_table", Column: "a_column"},
					},
				},
			},
		},
		{
			name: "sorts complex references",
			schema: Schema{
				Tables: []Table{
					{
						Name: "table_a",
						Columns: []Column{
							{Name: "id", Definition: "int", IsPrimary: true},
							{Name: "name", Definition: "text"},
						},
					},
					{
						Name: "table_c",
						Columns: []Column{
							{Name: "id", Definition: "int", IsPrimary: true},
							{Name: "b_id", Definition: "int"},
						},
					},
					{
						Name: "table_b",
						Columns: []Column{
							{Name: "id", Definition: "int", IsPrimary: true},
							{Name: "a_id", Definition: "int"},
							{Name: "name", Definition: "text"},
						},
					},
				},
				References: []Reference{
					{
						Source: TableColumn{Table: "table_a", Column: "name"},
						Target: TableColumn{Table: "table_b", Column: "name"},
					},
					{
						Source: TableColumn{Table: "table_c", Column: "b_id"},
						Target: TableColumn{Table: "table_b", Column: "id"},
					},
					{
						Source: TableColumn{Table: "table_b", Column: "a_id"},
						Target: TableColumn{Table: "table_a", Column: "id"},
					},
					{
						Source: TableColumn{Table: "table_c", Column: "id"},
						Target: TableColumn{Table: "table_a", Column: "id"},
					},
					{
						Source: TableColumn{Table: "table_a", Column: "id"},
						Target: TableColumn{Table: "table_b", Column: "a_id"},
					},
					{
						Source: TableColumn{Table: "table_a", Column: "id"},
						Target: TableColumn{Table: "table_c", Column: "id"},
					},
					{
						Source: TableColumn{Table: "table_b", Column: "id"},
						Target: TableColumn{Table: "table_c", Column: "b_id"},
					},
					{
						Source: TableColumn{Table: "table_c", Column: "b_id"},
						Target: TableColumn{Table: "table_a", Column: "id"},
					},
				},
			},
			expected: Schema{
				Tables: []Table{
					{
						Name: "table_a",
						Columns: []Column{
							{Name: "id", Definition: "int", IsPrimary: true},
							{Name: "name", Definition: "text"},
						},
					},
					{
						Name: "table_b",
						Columns: []Column{
							{Name: "id", Definition: "int", IsPrimary: true},
							{Name: "a_id", Definition: "int"},
							{Name: "name", Definition: "text"},
						},
					},
					{
						Name: "table_c",
						Columns: []Column{
							{Name: "id", Definition: "int", IsPrimary: true},
							{Name: "b_id", Definition: "int"},
						},
					},
				},
				References: []Reference{
					{
						Source: TableColumn{Table: "table_a", Column: "id"},
						Target: TableColumn{Table: "table_b", Column: "a_id"},
					},
					{
						Source: TableColumn{Table: "table_a", Column: "id"},
						Target: TableColumn{Table: "table_c", Column: "id"},
					},
					{
						Source: TableColumn{Table: "table_a", Column: "name"},
						Target: TableColumn{Table: "table_b", Column: "name"},
					},
					{
						Source: TableColumn{Table: "table_b", Column: "a_id"},
						Target: TableColumn{Table: "table_a", Column: "id"},
					},
					{
						Source: TableColumn{Table: "table_b", Column: "id"},
						Target: TableColumn{Table: "table_c", Column: "b_id"},
					},
					{
						Source: TableColumn{Table: "table_c", Column: "b_id"},
						Target: TableColumn{Table: "table_a", Column: "id"},
					},
					{
						Source: TableColumn{Table: "table_c", Column: "b_id"},
						Target: TableColumn{Table: "table_b", Column: "id"},
					},
					{
						Source: TableColumn{Table: "table_c", Column: "id"},
						Target: TableColumn{Table: "table_a", Column: "id"},
					},
				},
			},
		},
		{
			name: "sorts columns considering definition",
			schema: Schema{
				Tables: []Table{
					{
						Name: "test_table",
						Columns: []Column{
							{Name: "z_column", Definition: "text"},
							{Name: "a_column", Definition: "varchar(255)"},
							{Name: "id", Definition: "int", IsPrimary: true},
							{Name: "created_at", Definition: "timestamp"},
							{Name: "b_column", Definition: "text"},
							{Name: "updated_at", Definition: "timestamp"},
							{Name: "status", Definition: "varchar(50)"},
							{Name: "deleted_at", Definition: "timestamp"},
							{Name: "name", Definition: "varchar(255)"},
							{Name: "description", Definition: "text"},
							{Name: "version", Definition: "int"},
						},
					},
				},
			},
			expected: Schema{
				Tables: []Table{
					{
						Name: "test_table",
						Columns: []Column{
							{Name: "id", Definition: "int", IsPrimary: true},
							{Name: "version", Definition: "int"},
							{Name: "b_column", Definition: "text"},
							{Name: "description", Definition: "text"},
							{Name: "z_column", Definition: "text"},
							{Name: "created_at", Definition: "timestamp"},
							{Name: "deleted_at", Definition: "timestamp"},
							{Name: "updated_at", Definition: "timestamp"},
							{Name: "a_column", Definition: "varchar(255)"},
							{Name: "name", Definition: "varchar(255)"},
							{Name: "status", Definition: "varchar(50)"},
						},
					},
				},
			},
		},
		{
			name: "empty schema",
			schema: Schema{
				Tables:     []Table{},
				References: []Reference{},
			},
			expected: Schema{
				Tables:     []Table{},
				References: []Reference{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.schema.Sort()
			assert.Equal(t, tt.expected, tt.schema)
		})
	}
}

func TestSchema_SkipTables(t *testing.T) {
	t.Parallel()

	usersToGoose := Reference{
		Source: TableColumn{Table: "public.users", Column: "id"},
		Target: TableColumn{Table: "public.goose_db_version", Column: "id"},
	}
	usersToMigrations := Reference{
		Source: TableColumn{Table: "public.users", Column: "id"},
		Target: TableColumn{Table: "public.schema_migrations", Column: "id"},
	}
	usersToUsers := Reference{
		Source: TableColumn{Table: "public.users", Column: "manager_id"},
		Target: TableColumn{Table: "public.users", Column: "id"},
	}

	newSchema := func() Schema {
		return Schema{
			Tables: []Table{
				{Name: "public.users"},
				{Name: "public.goose_db_version"},
				{Name: "public.schema_migrations"},
				{Name: "audit.goose_db_version"},
			},
			References: []Reference{usersToGoose, usersToMigrations, usersToUsers},
		}
	}

	tests := []struct {
		name       string
		skipTables []string
		expected   Schema
	}{
		{
			name:       "unqualified name matches every schema",
			skipTables: []string{"goose_db_version"},
			expected: Schema{
				Tables: []Table{
					{Name: "public.users"},
					{Name: "public.schema_migrations"},
				},
				References: []Reference{usersToMigrations, usersToUsers},
			},
		},
		{
			name:       "qualified name matches one schema",
			skipTables: []string{"public.goose_db_version"},
			expected: Schema{
				Tables: []Table{
					{Name: "public.users"},
					{Name: "audit.goose_db_version"},
					{Name: "public.schema_migrations"},
				},
				References: []Reference{usersToMigrations, usersToUsers},
			},
		},
		{
			name:       "multiple names trim whitespace and ignore duplicates",
			skipTables: []string{" goose_db_version", "schema_migrations ", "goose_db_version", ""},
			expected: Schema{
				Tables:     []Table{{Name: "public.users"}},
				References: []Reference{usersToUsers},
			},
		},
		{
			name:       "unknown name leaves schema unchanged",
			skipTables: []string{"unknown"},
			expected:   newSchema(),
		},
		{
			name:       "matching is case-sensitive",
			skipTables: []string{"GOOSE_DB_VERSION"},
			expected:   newSchema(),
		},
		{
			name:       "empty input leaves schema unchanged",
			skipTables: []string{"", " "},
			expected:   newSchema(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actual := newSchema()
			actual.SkipTables(tt.skipTables...)

			assert.Equal(t, tt.expected, actual)
		})
	}
}
