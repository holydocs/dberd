package mongodb

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/denchenko/dberd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestExtractSchema(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	container, client := setupTestDB(t)
	defer func() {
		err := container.Terminate(context.Background())
		if err != nil {
			slog.Warn("terminating mongodb container", "error", err)
		}
	}()
	defer func() {
		err := client.Disconnect(ctx)
		if err != nil {
			slog.Warn("disconnecting mongo client", "error", err)
		}
	}()

	// Create test schema
	db := client.Database("test")
	collections := []struct {
		name string
		doc  bson.M
	}{
		{
			name: "users",
			doc: bson.M{
				"name":     "John Doe",
				"email":    "john@example.com",
				"age":      30,
				"active":   true,
				"tags":     []string{"user", "admin"},
				"settings": bson.M{"theme": "dark", "notifications": true},
			},
		},
		{
			name: "products",
			doc: bson.M{
				"name":       "Product 1",
				"price":      99.99,
				"in_stock":   true,
				"categories": []string{"electronics", "gadgets"},
				"attributes": bson.M{"color": "black", "weight": 1.5},
			},
		},
	}

	for _, coll := range collections {
		_, err := db.Collection(coll.name).InsertOne(ctx, coll.doc)
		require.NoError(t, err)
	}

	source := NewSourceFromClient(client)

	actual, err := source.ExtractSchema(ctx)
	require.NoError(t, err)

	actual.Sort()

	expected := dberd.Schema{
		Tables: []dberd.Table{
			{
				Name: "test.users",
				Columns: []dberd.Column{
					{Name: "_id", Definition: "ObjectId", IsPrimary: true},
					{Name: "active", Definition: "Boolean"},
					{Name: "age", Definition: "Int"},
					{Name: "email", Definition: "String"},
					{Name: "name", Definition: "String"},
					{Name: "settings", Definition: "Object"},
					{Name: "tags", Definition: "Array"},
				},
			},
			{
				Name: "test.products",
				Columns: []dberd.Column{
					{Name: "_id", Definition: "ObjectId", IsPrimary: true},
					{Name: "attributes", Definition: "Object"},
					{Name: "categories", Definition: "Array"},
					{Name: "in_stock", Definition: "Boolean"},
					{Name: "name", Definition: "String"},
					{Name: "price", Definition: "Double"},
				},
			},
		},
	}

	expected.Sort()

	assert.Equal(t, expected, actual)
}

func setupTestDB(t *testing.T) (testcontainers.Container, *mongo.Client) {
	ctx := context.Background()

	container, err := mongodb.RunContainer(ctx)
	require.NoError(t, err)

	connStr, err := container.ConnectionString(ctx)
	require.NoError(t, err)

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connStr))
	require.NoError(t, err)

	// Wait for the database to be ready
	require.Eventually(t, func() bool {
		err := client.Ping(ctx, nil)
		return err == nil
	}, 10*time.Second, 100*time.Millisecond)

	return container, client
}
