// Package mongodb provides functionality for extracting database schema information
// from MongoDB databases.
package mongodb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/denchenko/dberd"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Ensure Source implements dberd interfaces.
var (
	_ dberd.Source = (*Source)(nil)
)

// Source represents a MongoDB database source for schema extraction.
type Source struct {
	client *mongo.Client
	closer io.Closer
}

// NewSource creates a new MongoDB source from a connection string.
func NewSource(connStr string) (*Source, error) {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(connStr))
	if err != nil {
		return nil, fmt.Errorf("connecting to mongodb: %w", err)
	}

	return &Source{
		client: client,
		closer: &mongoCloser{client: client},
	}, nil
}

// NewSourceFromClient creates a new MongoDB source from an existing client.
// This is useful when you want to reuse an existing MongoDB client
// for schema extraction purposes.
func NewSourceFromClient(client *mongo.Client) *Source {
	return &Source{
		client: client,
	}
}

// Close closes the MongoDB client if it was created by NewSource.
// If the client was provided externally (via NewSourceFromClient), this is a no-op.
func (s *Source) Close() error {
	if s.closer == nil {
		return nil
	}

	return s.closer.Close()
}

// mongoCloser is a wrapper around mongo.Client that implements io.Closer
type mongoCloser struct {
	client *mongo.Client
}

func (c *mongoCloser) Close() error {
	return c.client.Disconnect(context.Background())
}

// ExtractSchema extracts the complete database schema including collections.
func (s *Source) ExtractSchema(ctx context.Context) (schema dberd.Schema, err error) {
	schema.Tables, err = s.extractCollections(ctx)
	if err != nil {
		return dberd.Schema{}, fmt.Errorf("extracting collections: %w", err)
	}

	return schema, nil
}

// extractCollections queries the database for collection information and converts it to dberd.Table format.
func (s *Source) extractCollections(ctx context.Context) ([]dberd.Table, error) {
	databases, err := s.client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("listing databases: %w", err)
	}

	var tables []dberd.Table
	for _, dbName := range databases {
		// Skip system databases
		if strings.HasPrefix(dbName, "system") || dbName == "admin" || dbName == "local" {
			continue
		}

		db := s.client.Database(dbName)
		collections, err := db.ListCollectionNames(ctx, bson.M{})
		if err != nil {
			return nil, fmt.Errorf("listing collections in database %s: %w", dbName, err)
		}

		for _, collName := range collections {
			// Skip system collections
			if strings.HasPrefix(collName, "system.") {
				continue
			}

			// Get collection schema
			coll := db.Collection(collName)
			schema, err := s.getCollectionSchema(ctx, coll)
			if err != nil {
				return nil, fmt.Errorf("getting schema for collection %s: %w", collName, err)
			}

			tables = append(tables, dberd.Table{
				Name:    fmt.Sprintf("%s.%s", dbName, collName),
				Columns: schema,
			})
		}
	}

	return tables, nil
}

// getCollectionSchema extracts the schema of a collection by sampling documents.
func (s *Source) getCollectionSchema(ctx context.Context, coll *mongo.Collection) ([]dberd.Column, error) {
	// Sample a document to infer schema
	var doc bson.M
	err := coll.FindOne(ctx, bson.M{}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			// Empty collection, return empty schema
			return nil, nil
		}
		return nil, fmt.Errorf("sampling document: %w", err)
	}

	// Convert BSON document to columns
	columns := make([]dberd.Column, 0, len(doc))
	for field, value := range doc {
		// Handle MongoDB-specific field
		if field == "_id" {
			columns = append(columns, dberd.Column{
				Name:       field,
				Definition: "ObjectId",
				IsPrimary:  true,
			})
			continue
		}

		// Determine field type
		columns = append(columns, dberd.Column{
			Name:       field,
			Definition: getMongoDBType(value),
		})
	}

	return columns, nil
}

// getMongoDBType determines the MongoDB type from a value.
func getMongoDBType(value interface{}) string {
	switch v := value.(type) {
	case string:
		return "String"
	case int, int32, int64:
		return "Int"
	case float32, float64:
		return "Double"
	case bool:
		return "Boolean"
	case []interface{}:
		return "Array"
	case primitive.A:
		return "Array"
	case bson.M, map[string]interface{}:
		return "Object"
	case nil:
		return "Null"
	default:
		return fmt.Sprintf("%T", v)
	}
}
