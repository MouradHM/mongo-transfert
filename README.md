# MongoDB Transfer Tool

A simple command-line tool for transferring collections between MongoDB databases.

## Features

- Transfer all collections or a specified subset from one MongoDB database to another
- Parallel processing of collections for faster transfers
- Configurable batch size for optimized performance
- Detailed transfer summary with success/failure status

## Installation

### Prerequisites

- Go 1.16 or higher
- Access to source and destination MongoDB instances

### Building from source

```bash
# Clone the repository
git clone git@github.com:MouradHM/mongo-transfert.git
cd mongotransfert

# Build the binary
go build -o mongotransfert ./cmd/mongotransfert
```

## Usage

```bash
./mongotransfert \
  --source "mongodb://user:pass@source-host:27017" \
  --dest "mongodb://user:pass@dest-host:27017" \
  --source-db "sourcedb" \
  --dest-db "destdb" \
  --collections "collection1,collection2,collection3" \
  --batch-size 200 \
  --workers 5
```

### Command-line Options

| Flag | Description | Default | Required |
|------|-------------|---------|----------|
| `--source` | Source MongoDB URI | - | Yes |
| `--dest` | Destination MongoDB URI | - | Yes |
| `--source-db` | Source database name | - | Yes |
| `--dest-db` | Destination database name | - | Yes |
| `--collections` | Comma-separated list of collections to transfer | All collections | No |
| `--batch-size` | Number of documents to transfer in a batch | 100 | No |
| `--workers` | Number of parallel workers for collection transfer | 3 | No |

## Examples

### Transfer all collections

```bash
./mongotransfert \
  --source "mongodb://localhost:27017" \
  --dest "mongodb://localhost:27017" \
  --source-db "sourcedb" \
  --dest-db "destdb"
```

### Transfer specific collections

```bash
./mongotransfert \
  --source "mongodb://localhost:27017" \
  --dest "mongodb://localhost:27017" \
  --source-db "sourcedb" \
  --dest-db "destdb" \
  --collections "users,orders,products"
```

### Optimize transfer with more workers and larger batch size

```bash
./mongotransfert \
  --source "mongodb://localhost:27017" \
  --dest "mongodb://localhost:27017" \
  --source-db "sourcedb" \
  --dest-db "destdb" \
  --batch-size 500 \
  --workers 8
```

## How It Works

1. The tool validates the connections to both source and destination MongoDB instances
2. It retrieves the list of collections to be transferred (either all or specified ones)
3. Multiple worker goroutines process the collections in parallel
4. For each collection, the tool:
   - Creates an index for efficient writes in the destination collection
   - Transfers documents in batches to optimize performance
   - Reports progress and results
5. A summary is displayed after all collections are processed

## License

MIT 