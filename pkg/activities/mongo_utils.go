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

	// Add performance optimizations
	clientOptions.SetMaxPoolSize(100)                  // Increase connection pool size
	clientOptions.SetMinPoolSize(10)                   // Maintain minimum connections
	clientOptions.SetMaxConnIdleTime(30 * time.Second) // Close idle connections after 30s
	clientOptions.SetRetryWrites(true)                 // Enable retry for failed writes
	clientOptions.SetRetryReads(true)                  // Enable retry for failed reads
	clientOptions.SetCompressors([]string{"snappy"})   // Enable compression

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

// getCollectionIndexes retrieves all indexes from a collection
func getCollectionIndexes(ctx context.Context, collection *mongo.Collection) ([]mongo.IndexModel, error) {
	cursor, err := collection.Indexes().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list indexes: %w", err)
	}
	defer cursor.Close(ctx)

	var indexes []mongo.IndexModel
	for cursor.Next(ctx) {
		var index bson.M
		if err := cursor.Decode(&index); err != nil {
			return nil, fmt.Errorf("failed to decode index: %w", err)
		}

		// Skip _id_ index as it's created automatically
		if index["name"] == "_id_" {
			continue
		}

		// Convert key from bson.M to bson.D to maintain key order
		keyM, ok := index["key"].(bson.M)
		if !ok {
			return nil, fmt.Errorf("invalid index key format")
		}

		// Convert M to D to preserve key order
		keyD := bson.D{}
		for k, v := range keyM {
			keyD = append(keyD, bson.E{Key: k, Value: v})
		}

		options := options.Index()

		// Set index options
		if unique, ok := index["unique"].(bool); ok {
			options.SetUnique(unique)
		}
		if sparse, ok := index["sparse"].(bool); ok {
			options.SetSparse(sparse)
		}
		if expireAfterSeconds, ok := index["expireAfterSeconds"].(int32); ok {
			options.SetExpireAfterSeconds(int32(expireAfterSeconds))
		}
		if partialFilterExpression, ok := index["partialFilterExpression"]; ok {
			options.SetPartialFilterExpression(partialFilterExpression)
		}

		indexes = append(indexes, mongo.IndexModel{
			Keys:    keyD,
			Options: options,
		})
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return indexes, nil
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
	overwriteDestination bool,
) (int, error) {
	if batchSize <= 0 {
		batchSize = 100 // Default batch size
	}

	sourceCollection := sourceClient.Database(sourceDB).Collection(collectionName)
	destCollection := destClient.Database(destDB).Collection(collectionName)

	// Check if destination collection exists
	collections, err := destClient.Database(destDB).ListCollectionNames(ctx, bson.M{"name": collectionName})
	if err != nil {
		return 0, fmt.Errorf("failed to check destination collection: %w", err)
	}

	collectionExists := len(collections) > 0
	if collectionExists && !overwriteDestination {
		return 0, fmt.Errorf("destination collection %s already exists and overwrite is not enabled", collectionName)
	}

	// Get source collection indexes
	indexes, err := getCollectionIndexes(ctx, sourceCollection)
	if err != nil {
		return 0, fmt.Errorf("failed to get source collection indexes: %w", err)
	}

	// Drop the destination collection if it exists and overwrite is enabled
	if collectionExists && overwriteDestination {
		log.Printf("Dropping existing collection %s in destination", collectionName)
		err = destCollection.Drop(ctx)
		if err != nil {
			return 0, fmt.Errorf("failed to drop destination collection: %w", err)
		}
	}

	// Create indexes in background for better performance
	if len(indexes) > 0 {
		for i := range indexes {
			if indexes[i].Options == nil {
				indexes[i].Options = options.Index()
			}
			indexes[i].Options.SetBackground(true) // Create indexes in background
		}
		_, err = destCollection.Indexes().CreateMany(ctx, indexes)
		if err != nil {
			return 0, fmt.Errorf("failed to create indexes: %w", err)
		}
		log.Printf("Created %d indexes for collection %s", len(indexes), collectionName)
	}

	// Count documents for reporting
	count, err := sourceCollection.CountDocuments(ctx, bson.D{})
	if err != nil {
		return 0, fmt.Errorf("failed to count documents: %w", err)
	}

	if count == 0 {
		return 0, nil // Nothing to transfer
	}

	// Optimize read performance
	findOptions := options.Find().
		SetNoCursorTimeout(true).      // Prevent cursor timeout
		SetAllowDiskUse(true).         // Allow disk use for large result sets
		SetBatchSize(int32(batchSize)) // Match batch size for optimal performance

	// Retrieve and insert documents in batches
	cursor, err := sourceCollection.Find(ctx, bson.D{}, findOptions)
	if err != nil {
		return 0, fmt.Errorf("failed to execute find: %w", err)
	}
	defer cursor.Close(ctx)

	totalTransferred := 0
	batch := make([]interface{}, 0, batchSize)

	// Optimize write performance
	insertOptions := options.InsertMany().
		SetOrdered(false) // Allow unordered inserts for better performance

	for cursor.Next(ctx) {
		var document bson.M
		err := cursor.Decode(&document)
		if err != nil {
			return totalTransferred, fmt.Errorf("failed to decode document: %w", err)
		}

		batch = append(batch, document)

		// Insert batch when it reaches the batch size
		if len(batch) >= batchSize {
			_, err = destCollection.InsertMany(ctx, batch, insertOptions)
			if err != nil {
				return totalTransferred, fmt.Errorf("failed to insert batch: %w", err)
			}
			totalTransferred += len(batch)
			batch = make([]interface{}, 0, batchSize)

			// Log progress for large collections
			if count > 10000 && int64(totalTransferred)%(count/10) < int64(batchSize) {
				log.Printf("Progress for %s: %d/%d documents (%.1f%%)",
					collectionName, totalTransferred, count,
					float64(totalTransferred)/float64(count)*100)
			}
		}
	}

	// Insert the remaining documents
	if len(batch) > 0 {
		_, err = destCollection.InsertMany(ctx, batch, insertOptions)
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
