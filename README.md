# Nexus Storage

Nexus Storage is a storage layer for unstructured and semi-structured data.

## Folder Structure

The project is organized as follows:

```
nexus-store/
├── app/
│ ├── migrate/ # Database migration tool
│ └── server/ # Main server application
├── db/
│ └── migration/ # SQL migration files
├── pkg/
│ ├── dbaccess/ # Database access layer including sqlc sql files
│ ├── internal/ # Internal packages
│ ├── migrate/ # Database Migrator
│ └── proto/ # Generated gRPC code
└── proto/
  └── nexus_store/ # Protocol Buffer definitions
```

## Getting Started

### Prerequisites

- Go 1.23.1 or later
- PostgreSQL 14.11 or later

### Installation

1. Install dependencies:

   ```bash
   go mod download
   ```

2. Set up the database:

   - Create a PostgreSQL database
   - Set the database connection details in your environment variables or `.env` file

3. Run database migrations:

   ```bash
   go run app/migrate/main.go up
   ```

4. Build and run the server:

   ```bash
   make
   ```

### Running Tests

```bash
make test
```
