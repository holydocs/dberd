package dberd

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
					{Name: "public.schema_migrations"},
					{Name: "audit.goose_db_version"},
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

func TestSchema_AddLogicalReferences(t *testing.T) {
	t.Parallel()

	table := func(name string, columns ...string) Table {
		result := Table{
			Name:    name,
			Columns: make([]Column, 0, len(columns)),
		}
		for _, column := range columns {
			result.Columns = append(result.Columns, Column{Name: column})
		}
		return result
	}
	reference := func(sourceTable, sourceColumn, targetTable string) Reference {
		return Reference{
			Source: TableColumn{Table: sourceTable, Column: sourceColumn},
			Target: TableColumn{Table: targetTable, Column: "id"},
			Name:   "logical " + sourceColumn,
		}
	}

	tests := []struct {
		name       string
		schema     Schema
		skipTables []string
		repeat     bool
		sort       bool
		expected   []Reference
	}{
		{
			name: "infers exact and regular plural names",
			schema: Schema{Tables: []Table{
				table("account", "id"),
				table("users", "id"),
				table("events", "account_id", "user_id"),
			}},
			expected: []Reference{
				reference("events", "account_id", "account"),
				reference("events", "user_id", "users"),
			},
		},
		{
			name: "infers ies plural from singular and exact plural stems",
			schema: Schema{Tables: []Table{
				table("categories", "id"),
				table("posts", "category_id", "categories_id"),
			}},
			expected: []Reference{
				reference("posts", "category_id", "categories"),
				reference("posts", "categories_id", "categories"),
			},
		},
		{
			name: "infers common es plurals from singular stems",
			schema: Schema{Tables: []Table{
				table("addresses", "id"),
				table("boxes", "id"),
				table("dishes", "id"),
				table("quizzes", "id"),
				table("watches", "id"),
				table("deliveries", "address_id", "box_id", "dish_id", "quiz_id", "watch_id"),
			}},
			expected: []Reference{
				reference("deliveries", "address_id", "addresses"),
				reference("deliveries", "box_id", "boxes"),
				reference("deliveries", "dish_id", "dishes"),
				reference("deliveries", "quiz_id", "quizzes"),
				reference("deliveries", "watch_id", "watches"),
			},
		},
		{
			name: "matches and preserves the same qualifier",
			schema: Schema{Tables: []Table{
				table("public.posts", "user_id"),
				table("public.users", "id"),
				table("audit.users", "id"),
			}},
			expected: []Reference{
				reference("public.posts", "user_id", "public.users"),
			},
		},
		{
			name: "does not fall back across qualifiers",
			schema: Schema{Tables: []Table{
				table("public.posts", "account_id"),
				table("audit.accounts", "id"),
			}},
		},
		{
			name: "does not infer an ambiguous alias",
			schema: Schema{Tables: []Table{
				table("public.posts", "category_id"),
				table("public.category", "id"),
				table("public.categories", "id"),
			}},
		},
		{
			name: "requires an exact id column but not a primary key",
			schema: Schema{Tables: []Table{
				table("events", "account_id", "user_id"),
				table("account", "id"),
				table("users", "ID"),
			}},
			expected: []Reference{
				reference("events", "account_id", "account"),
			},
		},
		{
			name: "matches source column names case sensitively",
			schema: Schema{Tables: []Table{
				table("events", "User_id", "_id", "userID", "parent_id"),
				table("users", "id"),
			}},
		},
		{
			name: "keeps explicit references authoritative",
			schema: Schema{
				Tables: []Table{
					table("posts", "category_id", "user_id"),
					table("categories", "id"),
					table("users", "id"),
				},
				References: []Reference{
					reference("posts", "category_id", "taxonomy.tags"),
				},
			},
			expected: []Reference{
				reference("posts", "category_id", "taxonomy.tags"),
				reference("posts", "user_id", "users"),
			},
		},
		{
			name: "is idempotent and permits a unique self reference",
			schema: Schema{Tables: []Table{
				table("categories", "id", "category_id"),
			}},
			repeat: true,
			expected: []Reference{
				reference("categories", "category_id", "categories"),
			},
		},
		{
			name: "does not target a skipped table",
			schema: Schema{Tables: []Table{
				table("posts", "category_id"),
				table("categories", "id"),
			}},
			skipTables: []string{"categories"},
		},
		{
			name: "sorts inferred references with existing schema ordering",
			schema: Schema{Tables: []Table{
				table("posts", "user_id", "category_id"),
				table("users", "id"),
				table("categories", "id"),
			}},
			sort: true,
			expected: []Reference{
				reference("posts", "category_id", "categories"),
				reference("posts", "user_id", "users"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actual := tt.schema
			actual.SkipTables(tt.skipTables...)
			actual.AddLogicalReferences()
			if tt.repeat {
				actual.AddLogicalReferences()
			}
			if tt.sort {
				actual.Sort()
			}

			assert.Equal(t, tt.expected, actual.References)
		})
	}
}

func TestParsePredefinedLogicalReferences(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		yaml     string
		expected []Reference
		wantErr  bool
	}{
		{
			name: "parses fully qualified non-id references",
			yaml: `references:
  - source:
      table: public.orders
      column: billing_contact
    target:
      table: public.contacts
      column: external_id
    name: billing contact
`,
			expected: []Reference{{
				Source: TableColumn{Table: "public.orders", Column: "billing_contact"},
				Target: TableColumn{Table: "public.contacts", Column: "external_id"},
				Name:   "billing contact",
			}},
		},
		{
			name: "keeps an omitted name empty",
			yaml: `references:
  - source: {table: public.orders, column: billing_contact}
    target: {table: public.contacts, column: external_id}
`,
			expected: []Reference{{
				Source: TableColumn{Table: "public.orders", Column: "billing_contact"},
				Target: TableColumn{Table: "public.contacts", Column: "external_id"},
			}},
		},
		{
			name:    "rejects malformed yaml",
			yaml:    "references: [",
			wantErr: true,
		},
		{
			name: "rejects unknown fields",
			yaml: `references:
  - source: {table: orders, column: contact_id, unexpected: value}
    target: {table: contacts, column: id}
`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actual, err := ParsePredefinedLogicalReferences(strings.NewReader(tt.yaml))

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestSchema_AddPredefinedLogicalReferences(t *testing.T) {
	t.Parallel()

	table := func(name string, columns ...string) Table {
		result := Table{Name: name, Columns: make([]Column, 0, len(columns))}
		for _, column := range columns {
			result.Columns = append(result.Columns, Column{Name: column})
		}
		return result
	}

	configuredReference := Reference{
		Source: TableColumn{Table: "public.orders", Column: "billing_contact"},
		Target: TableColumn{Table: "public.contacts", Column: "external_id"},
	}

	tests := []struct {
		name       string
		schema     Schema
		references []Reference
		expected   []Reference
		wantErr    string
	}{
		{
			name: "adds a valid reference and ignores an exact duplicate",
			schema: Schema{Tables: []Table{
				table("public.orders", "billing_contact"),
				table("public.contacts", "external_id"),
			}},
			references: []Reference{configuredReference, configuredReference},
			expected:   []Reference{configuredReference},
		},
		{
			name: "does not change schema when any endpoint is missing",
			schema: Schema{Tables: []Table{
				table("public.orders", "billing_contact"),
				table("public.contacts", "external_id"),
			}},
			references: []Reference{
				configuredReference,
				{Source: TableColumn{Table: "public.orders", Column: "missing"}, Target: configuredReference.Target},
			},
			wantErr: "source column public.orders.missing does not exist",
		},
		{
			name: "rejects a different target for an existing source",
			schema: Schema{
				Tables: []Table{
					table("public.orders", "billing_contact"),
					table("public.contacts", "external_id"),
					table("public.legacy_contacts", "id"),
				},
				References: []Reference{{
					Source: configuredReference.Source,
					Target: TableColumn{Table: "public.legacy_contacts", Column: "id"},
				}},
			},
			references: []Reference{configuredReference},
			expected: []Reference{{
				Source: configuredReference.Source,
				Target: TableColumn{Table: "public.legacy_contacts", Column: "id"},
			}},
			wantErr: "source column public.orders.billing_contact already references public.legacy_contacts.id",
		},
		{
			name: "ignores an exact duplicate when the schema already contains duplicates",
			schema: Schema{
				Tables: []Table{
					table("public.orders", "billing_contact"),
					table("public.contacts", "external_id"),
				},
				References: []Reference{configuredReference, configuredReference},
			},
			references: []Reference{configuredReference},
			expected:   []Reference{configuredReference, configuredReference},
		},
		{
			name: "ignores an exact duplicate with a different name",
			schema: Schema{
				Tables: []Table{
					table("public.orders", "billing_contact"),
					table("public.contacts", "external_id"),
				},
				References: []Reference{{
					Source: configuredReference.Source,
					Target: configuredReference.Target,
					Name:   "existing name",
				}},
			},
			references: []Reference{{
				Source: configuredReference.Source,
				Target: configuredReference.Target,
				Name:   "configured name",
			}},
			expected: []Reference{{
				Source: configuredReference.Source,
				Target: configuredReference.Target,
				Name:   "existing name",
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actual := tt.schema
			err := actual.AddPredefinedLogicalReferences(tt.references)

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.expected, actual.References)
		})
	}
}
