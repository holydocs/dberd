package dberd

import (
	"errors"
	"fmt"
	"io"

	"gopkg.in/yaml.v3"
)

type predefinedLogicalReferences struct {
	References []Reference `yaml:"references"`
}

// ParsePredefinedLogicalReferences decodes predefined logical references from YAML.
func ParsePredefinedLogicalReferences(r io.Reader) ([]Reference, error) {
	decoder := yaml.NewDecoder(r)
	decoder.KnownFields(true)

	var configuration predefinedLogicalReferences
	if err := decoder.Decode(&configuration); err != nil {
		return nil, fmt.Errorf("decoding predefined logical references: %w", err)
	}

	var extraDocument any
	if err := decoder.Decode(&extraDocument); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("decoding predefined logical references: multiple YAML documents")
		}
		return nil, fmt.Errorf("decoding predefined logical references: %w", err)
	}

	return configuration.References, nil
}

// AddPredefinedLogicalReferences adds explicitly configured references.
// Each endpoint must exist in the schema, and existing references take precedence.
func (s *Schema) AddPredefinedLogicalReferences(references []Reference) error {
	tableColumns := make(map[string]map[string]struct{}, len(s.Tables))
	for _, table := range s.Tables {
		columns := make(map[string]struct{}, len(table.Columns))
		for _, column := range table.Columns {
			columns[column.Name] = struct{}{}
		}
		tableColumns[table.Name] = columns
	}

	existingTargets := make(map[TableColumn][]TableColumn, len(s.References))
	for _, reference := range s.References {
		existingTargets[reference.Source] = append(existingTargets[reference.Source], reference.Target)
	}

	pendingTargets := make(map[TableColumn]TableColumn, len(references))
	toAdd := make([]Reference, 0, len(references))
	var validationErrors []error

	for _, reference := range references {
		if err := validateReferenceEndpoint(tableColumns, "source", reference.Source); err != nil {
			validationErrors = append(validationErrors, err)
		}
		if err := validateReferenceEndpoint(tableColumns, "target", reference.Target); err != nil {
			validationErrors = append(validationErrors, err)
		}
		if len(validationErrors) > 0 {
			continue
		}

		if targets, ok := existingTargets[reference.Source]; ok {
			conflictingTarget, hasConflict := conflictingTableColumn(targets, reference.Target)
			if !hasConflict {
				continue
			}
			validationErrors = append(validationErrors, fmt.Errorf(
				"source column %s.%s already references %s.%s",
				reference.Source.Table,
				reference.Source.Column,
				conflictingTarget.Table,
				conflictingTarget.Column,
			))
			continue
		}

		if target, ok := pendingTargets[reference.Source]; ok {
			if target == reference.Target {
				continue
			}
			validationErrors = append(validationErrors, fmt.Errorf(
				"source column %s.%s already references %s.%s",
				reference.Source.Table,
				reference.Source.Column,
				target.Table,
				target.Column,
			))
			continue
		}

		pendingTargets[reference.Source] = reference.Target
		toAdd = append(toAdd, reference)
	}

	if len(validationErrors) > 0 {
		return errors.Join(validationErrors...)
	}

	s.References = append(s.References, toAdd...)
	return nil
}

func validateReferenceEndpoint(tableColumns map[string]map[string]struct{}, endpointName string, endpoint TableColumn) error {
	columns, ok := tableColumns[endpoint.Table]
	if !ok {
		return fmt.Errorf("%s table %s does not exist", endpointName, endpoint.Table)
	}
	if _, ok := columns[endpoint.Column]; !ok {
		return fmt.Errorf("%s column %s.%s does not exist", endpointName, endpoint.Table, endpoint.Column)
	}
	return nil
}

func conflictingTableColumn(columns []TableColumn, column TableColumn) (TableColumn, bool) {
	for _, candidate := range columns {
		if candidate != column {
			return candidate, true
		}
	}
	return TableColumn{}, false
}
