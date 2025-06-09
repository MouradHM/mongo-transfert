package activities

import (
	"context"
	"fmt"
	"log"

	"github.com/mouradhm/mongo-transfert/pkg/models"
)

// ValidateConnections validates the MongoDB connections
func ValidateConnections(ctx context.Context, params models.TransferParams) error {
	log.Println("Validating MongoDB connections")

	// Validate source connection
	sourceClient, err := connectToMongoDB(ctx, params.SourceURI)
	if err != nil {
		return fmt.Errorf("failed to connect to source MongoDB: %w", err)
	}
	defer func() {
		if err := sourceClient.Disconnect(ctx); err != nil {
			log.Printf("Error disconnecting from source MongoDB: %v", err)
		}
	}()

	// Validate destination connection
	destClient, err := connectToMongoDB(ctx, params.DestinationURI)
	if err != nil {
		return fmt.Errorf("failed to connect to destination MongoDB: %w", err)
	}
	defer func() {
		if err := destClient.Disconnect(ctx); err != nil {
			log.Printf("Error disconnecting from destination MongoDB: %v", err)
		}
	}()

	log.Println("Successfully validated MongoDB connections")
	return nil
}

// GetCollections gets collections from the source database
func GetCollections(ctx context.Context, params models.TransferParams) ([]string, error) {
	log.Printf("Getting collections from source database: %s", params.SourceDB)

	// Connect to source MongoDB
	sourceClient, err := connectToMongoDB(ctx, params.SourceURI)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to source MongoDB: %w", err)
	}
	defer func() {
		if err := sourceClient.Disconnect(ctx); err != nil {
			log.Printf("Error disconnecting from source MongoDB: %v", err)
		}
	}()

	var collections []string
	if len(params.Collections) > 0 {
		// User specified collections to transfer
		collections = params.Collections
	} else {
		// Get all collections from the database
		collections, err = getCollectionsList(ctx, sourceClient, params.SourceDB)
		if err != nil {
			return nil, fmt.Errorf("failed to get collections list: %w", err)
		}
	}

	if len(collections) == 0 {
		log.Println("No collections found in the source database")
	} else {
		log.Printf("Found %d collections in source database", len(collections))
	}

	return collections, nil
}

// TransferCollection transfers a single collection from source to destination
func TransferCollection(ctx context.Context, params models.TransferParams, collectionName string) (models.CollectionTransferResult, error) {
	result := models.CollectionTransferResult{
		CollectionName: collectionName,
		Success:        false,
	}

	log.Printf("Starting transfer of collection: %s", collectionName)

	// Connect to source MongoDB
	sourceClient, err := connectToMongoDB(ctx, params.SourceURI)
	if err != nil {
		result.ErrorMessage = fmt.Sprintf("Failed to connect to source MongoDB: %v", err)
		return result, err
	}
	defer func() {
		if err := sourceClient.Disconnect(ctx); err != nil {
			log.Printf("Error disconnecting from source MongoDB: %v", err)
		}
	}()

	// Connect to destination MongoDB
	destClient, err := connectToMongoDB(ctx, params.DestinationURI)
	if err != nil {
		result.ErrorMessage = fmt.Sprintf("Failed to connect to destination MongoDB: %v", err)
		return result, err
	}
	defer func() {
		if err := destClient.Disconnect(ctx); err != nil {
			log.Printf("Error disconnecting from destination MongoDB: %v", err)
		}
	}()

	// Transfer the collection
	count, err := transferCollection(
		ctx,
		sourceClient,
		destClient,
		params.SourceDB,
		params.DestinationDB,
		collectionName,
		params.BatchSize,
		params.OverwriteDestination,
	)

	if err != nil {
		result.ErrorMessage = fmt.Sprintf("Failed to transfer collection: %v", err)
		return result, err
	}

	result.DocumentsCount = count
	result.Success = true
	log.Printf("Collection transfer completed: %s, %d documents", collectionName, count)

	return result, nil
}
