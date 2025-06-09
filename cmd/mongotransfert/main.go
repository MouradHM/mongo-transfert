package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/mouradhm/mongo-transfert/pkg/activities"
	"github.com/mouradhm/mongo-transfert/pkg/models"
)

func main() {
	// Define command-line flags
	sourceURI := flag.String("source", "", "Source MongoDB URI (required)")
	destURI := flag.String("dest", "", "Destination MongoDB URI (required)")
	sourceDB := flag.String("source-db", "", "Source database name (required)")
	destDB := flag.String("dest-db", "", "Destination database name (required)")
	collections := flag.String("collections", "", "Comma-separated list of collections to transfer (optional, default: all)")
	batchSize := flag.Int("batch-size", 100, "Number of documents to transfer in a batch")
	workerCount := flag.Int("workers", 3, "Number of parallel workers for collection transfer")
	overwrite := flag.Bool("overwrite", false, "Overwrite existing collections in destination (default: false)")

	// Parse command-line flags
	flag.Parse()

	// Validate required flags
	if *sourceURI == "" || *destURI == "" || *sourceDB == "" || *destDB == "" {
		fmt.Println("Error: missing required arguments")
		fmt.Println("Usage:")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Create transfer parameters
	params := models.TransferParams{
		SourceURI:            *sourceURI,
		DestinationURI:       *destURI,
		SourceDB:             *sourceDB,
		DestinationDB:        *destDB,
		BatchSize:            *batchSize,
		OverwriteDestination: *overwrite,
	}

	// Parse collections if provided
	if *collections != "" {
		params.Collections = parseCommaSeparatedList(*collections)
	}

	// Run the transfer process
	result, err := runTransfer(params, *workerCount)
	if err != nil {
		log.Fatalf("Transfer failed: %v", err)
	}

	// Print summary
	printSummary(result)

	if !result.OverallSuccess {
		os.Exit(1)
	}
}

// parseCommaSeparatedList parses a comma-separated string into a slice of strings
func parseCommaSeparatedList(input string) []string {
	if input == "" {
		return nil
	}

	var result []string
	for _, s := range splitCommas(input) {
		if s != "" {
			result = append(result, s)
		}
	}
	return result
}

// splitCommas splits a string by commas, handling whitespace
func splitCommas(s string) []string {
	var result []string
	var current string

	for _, c := range s {
		if c == ',' {
			result = append(result, current)
			current = ""
		} else if c != ' ' && c != '\t' {
			current += string(c)
		}
	}

	if current != "" {
		result = append(result, current)
	}

	return result
}

// runTransfer runs the MongoDB transfer process
func runTransfer(params models.TransferParams, workerCount int) (models.TransferResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Hour)
	defer cancel()

	result := models.TransferResult{
		OverallSuccess: true,
	}

	// Validate connections
	log.Println("Validating MongoDB connections...")
	err := activities.ValidateConnections(ctx, params)
	if err != nil {
		return result, fmt.Errorf("connection validation failed: %w", err)
	}

	// Get collections list
	log.Println("Getting collections list...")
	collections, err := activities.GetCollections(ctx, params)
	if err != nil {
		return result, fmt.Errorf("failed to get collections: %w", err)
	}

	if len(collections) == 0 {
		log.Println("No collections to transfer")
		return result, nil
	}

	log.Printf("Starting transfer of %d collections with %d workers", len(collections), workerCount)

	// Set up worker pool for collection transfer
	if workerCount <= 0 {
		workerCount = 3 // Default number of workers
	}

	// Create a channel for collection names
	collectionCh := make(chan string, len(collections))
	for _, collection := range collections {
		collectionCh <- collection
	}
	close(collectionCh)

	// Create a channel for results
	resultCh := make(chan models.CollectionTransferResult, len(collections))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()

			for collName := range collectionCh {
				log.Printf("Worker %d: Processing collection %s", workerId, collName)
				res, err := activities.TransferCollection(ctx, params, collName)
				if err != nil {
					log.Printf("Worker %d: Error transferring collection %s: %v", workerId, collName, err)
				}
				resultCh <- res
			}
		}(i)
	}

	// Wait for all workers to finish
	wg.Wait()
	close(resultCh)

	// Collect results
	totalDocs := 0
	allSuccessful := true
	for res := range resultCh {
		result.CollectionResults = append(result.CollectionResults, res)
		totalDocs += res.DocumentsCount
		if !res.Success {
			allSuccessful = false
		}
	}

	result.TotalDocuments = totalDocs
	result.OverallSuccess = allSuccessful

	return result, nil
}

// printSummary prints a summary of the transfer results
func printSummary(result models.TransferResult) {
	fmt.Println("\n=== MongoDB Transfer Summary ===")
	fmt.Printf("Total documents transferred: %d\n", result.TotalDocuments)
	fmt.Printf("Success: %v\n", result.OverallSuccess)
	fmt.Println("\nCollection details:")

	successCount := 0
	for _, collResult := range result.CollectionResults {
		status := "✓ Success"
		if !collResult.Success {
			status = "✗ Failed: " + collResult.ErrorMessage
		} else {
			successCount++
		}
		fmt.Printf("  - %s: %d documents, %s\n", collResult.CollectionName, collResult.DocumentsCount, status)
	}

	fmt.Printf("\nSuccessfully transferred %d out of %d collections\n", successCount, len(result.CollectionResults))
}
