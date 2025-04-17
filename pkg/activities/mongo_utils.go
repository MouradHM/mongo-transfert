package activities

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// connectToMongoDB establishes a connection to MongoDB with the given URI
func connectToMongoDB(ctx context.Context, uri string) (*mongo.Client, error) {
	clientOptions := options.Client().ApplyURI(uri)
	clientOptions.SetConnectTimeout(10 * time.Second)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Ping the database to verify connection
	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	return client, nil
}

// getCollectionsList retrieves the list of collections from a database
func getCollectionsList(ctx context.Context, client *mongo.Client, dbName string) ([]string, error) {
	db := client.Database(dbName)
	collections, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("failed to list collections: %w", err)
	}
	return collections, nil
}

// transferCollection transfers a single collection from source to destination
func transferCollection(
	ctx context.Context,
	sourceClient *mongo.Client,
	destClient *mongo.Client,
	sourceDB string,
	destDB string,
	collectionName string,
	batchSize int,
) (int, error) {
	if batchSize <= 0 {
		batchSize = 100 // Default batch size
	}

	sourceCollection := sourceClient.Database(sourceDB).Collection(collectionName)
	destCollection := destClient.Database(destDB).Collection(collectionName)

	// Create an index for efficient writes
	indexModel := mongo.IndexModel{
		Keys: bson.D{{Key: "_id", Value: 1}},
	}

	// Drop the destination collection if it exists
	err := destCollection.Drop(ctx)
	if err != nil {
		log.Printf("Warning: could not drop destination collection %s: %v", collectionName, err)
	}

	// Create the index
	_, err = destCollection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return 0, fmt.Errorf("failed to create index: %w", err)
	}

	// Count documents for reporting
	count, err := sourceCollection.CountDocuments(ctx, bson.D{})
	if err != nil {
		return 0, fmt.Errorf("failed to count documents: %w", err)
	}

	if count == 0 {
		return 0, nil // Nothing to transfer
	}

	// Retrieve and insert documents in batches
	cursor, err := sourceCollection.Find(ctx, bson.D{})
	if err != nil {
		return 0, fmt.Errorf("failed to execute find: %w", err)
	}
	defer cursor.Close(ctx)

	totalTransferred := 0
	batch := make([]interface{}, 0, batchSize)

	for cursor.Next(ctx) {
		var document bson.M
		err := cursor.Decode(&document)
		if err != nil {
			return totalTransferred, fmt.Errorf("failed to decode document: %w", err)
		}

		batch = append(batch, document)

		// Insert batch when it reaches the batch size
		if len(batch) >= batchSize {
			_, err = destCollection.InsertMany(ctx, batch)
			if err != nil {
				return totalTransferred, fmt.Errorf("failed to insert batch: %w", err)
			}
			totalTransferred += len(batch)
			batch = make([]interface{}, 0, batchSize)
		}
	}

	// Insert the remaining documents
	if len(batch) > 0 {
		_, err = destCollection.InsertMany(ctx, batch)
		if err != nil {
			return totalTransferred, fmt.Errorf("failed to insert final batch: %w", err)
		}
		totalTransferred += len(batch)
	}

	if err := cursor.Err(); err != nil {
		return totalTransferred, fmt.Errorf("cursor error: %w", err)
	}

	return totalTransferred, nil
}
