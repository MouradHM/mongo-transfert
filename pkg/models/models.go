package models

// TransferParams contains the parameters needed for MongoDB collection transfer
type TransferParams struct {
	SourceURI      string   `json:"sourceUri"`
	DestinationURI string   `json:"destinationUri"`
	SourceDB       string   `json:"sourceDb"`
	DestinationDB  string   `json:"destinationDb"`
	Collections    []string `json:"collections"`
	BatchSize      int      `json:"batchSize,omitempty"`
}

// CollectionTransferResult contains the result of a single collection transfer
type CollectionTransferResult struct {
	CollectionName string `json:"collectionName"`
	DocumentsCount int    `json:"documentsCount"`
	Success        bool   `json:"success"`
	ErrorMessage   string `json:"errorMessage,omitempty"`
}

// TransferResult contains the overall result of the MongoDB transfer workflow
type TransferResult struct {
	CollectionResults []CollectionTransferResult `json:"collectionResults"`
	OverallSuccess    bool                       `json:"overallSuccess"`
	TotalDocuments    int                        `json:"totalDocuments"`
}
