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
