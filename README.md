# LoadService API

Async FastAPI service for document and topic management with MinIO storage and Kafka integration.

## Features

- **Document Management**: Upload, download, delete documents (PDF, PPTX, DOCX)
- **Topic Management**: Create and manage topics with access control
- **User Access Control**: Manage user permissions (owner/reader roles)
- **MinIO Integration**: Object storage for documents
- **Kafka Integration**: Message streaming for document processing
- **PostgreSQL Database**: Async database operations with SQLAlchemy
- **System Topics**: Pre-configured system topics with special access rules

## Architecture

### Tech Stack

- **FastAPI**: Modern async web framework
- **PostgreSQL**: Relational database
- **MinIO**: S3-compatible object storage
- **Kafka**: Message broker for async processing
- **SQLAlchemy 2.0**: Async ORM
- **Alembic**: Database migrations
- **Pydantic**: Data validation

### Database Schema

- **users**: User accounts
- **topics**: Document collections (user-created or system)
- **topic_access**: User permissions for topics
- **documents**: Document metadata and status

## Installation

### Using Docker Compose (Recommended)

1. Clone the repository:
```bash
git clone <repository-url>
cd LoadService
```

2. Copy environment file:
```bash
cp .env.example .env
```

3. Start all services:
```bash
docker-compose up -d
```

The API will be available at `http://localhost:8000`

### Manual Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up PostgreSQL, MinIO, and Kafka

3. Configure environment variables in `.env`

4. Run migrations:
```bash
alembic upgrade head
```

5. Start the application:
```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## API Endpoints

### Health Check

#### GET `/`
Get service status.

**Response:**
```json
{
  "service": "LoadService",
  "status": "running",
  "version": "1.0.0"
}
```

#### GET `/health`
Health check endpoint.

**Response:**
```json
{
  "status": "healthy",
  "service": "LoadService"
}
```

### Document Endpoints

#### POST `/documents/upload`
Upload a document to a topic.

**Request:**
- Headers: `Authorization: Bearer <JWT_TOKEN>`
- Body: `multipart/form-data`
  - `file`: Document file (PDF, PPTX, or DOCX)
  - `topic_id`: UUID of the topic

**Response (201):**
```json
{
  "id": "uuid",
  "topic_id": "uuid",
  "filename": "document.pdf",
  "status": "pending",
  "created_at": "2026-01-02T00:00:00Z"
}
```

**Access Rules:**
- **User Topics**: Requires owner role
- **System Topics**: Requires system role with matching user ID (1-5)

#### DELETE `/documents/{doc_id}`
Delete a document.

**Request:**
- Headers: `Authorization: Bearer <JWT_TOKEN>`
- Path: `doc_id` (UUID)

**Response (204):** No content

**Access Rules:**
- Same as upload

#### GET `/documents/{doc_id}/status`
Get document processing status.

**Request:**
- Headers: `Authorization: Bearer <JWT_TOKEN>`
- Path: `doc_id` (UUID)

**Response (200):**
```json
{
  "id": "uuid",
  "filename": "document.pdf",
  "status": "pending" // or "success"
}
```

#### GET `/documents/{doc_id}/download`
Get presigned URL for downloading a document.

**Request:**
- Headers: `Authorization: Bearer <JWT_TOKEN>`
- Path: `doc_id` (UUID)

**Response (200):**
```json
{
  "download_url": "https://minio:9000/bucket/object?presigned-params"
}
```

**Access Rules:**
- Requires read or owner access to the topic
- System topics are accessible to all users

### Topic Endpoints

#### POST `/topics`
Create a new topic.

**Request:**
- Headers: `Authorization: Bearer <JWT_TOKEN>`
- Body:
```json
{
  "name": "My Topic"
}
```

**Response (201):**
```json
{
  "id": "uuid",
  "name": "My Topic",
  "is_system": false,
  "owner_id": "uuid",
  "created_at": "2026-01-02T00:00:00Z"
}
```

#### DELETE `/topics/{topic_id}`
Delete a topic.

**Request:**
- Headers: `Authorization: Bearer <JWT_TOKEN>`
- Path: `topic_id` (UUID)

**Response (204):** No content

**Access Rules:**
- Only topic owner can delete
- System topics cannot be deleted

#### GET `/topics/editable`
Get list of topics available for editing.

**Request:**
- Headers: `Authorization: Bearer <JWT_TOKEN>`

**Response (200):**
```json
[
  {
    "id": "uuid",
    "name": "My Topic",
    "is_system": false
  }
]
```

Returns all topics where the user has owner access.

#### GET `/topics/readable`
Get list of topics available for reading.

**Request:**
- Headers: `Authorization: Bearer <JWT_TOKEN>`

**Response (200):**
```json
[
  {
    "id": "uuid",
    "name": "My Topic",
    "is_system": false
  },
  {
    "id": "uuid",
    "name": "system_1",
    "is_system": true
  }
]
```

Returns all topics with any access plus all system topics.

#### GET `/topics/{topic_id}/documents`
Get list of documents in a topic.

**Request:**
- Headers: `Authorization: Bearer <JWT_TOKEN>`
- Path: `topic_id` (UUID)

**Response (200):**
```json
[
  {
    "id": "uuid",
    "filename": "document.pdf",
    "status": "success",
    "created_at": "2026-01-02T00:00:00Z"
  }
]
```

### User Access Endpoints

#### GET `/topics/{topic_id}/users`
Get list of users with access to a topic.

**Request:**
- Headers: `Authorization: Bearer <JWT_TOKEN>`
- Path: `topic_id` (UUID)

**Response (200):**
```json
[
  {
    "user_id": "uuid",
    "username": "john_doe",
    "role": "owner"
  },
  {
    "user_id": "uuid",
    "username": "jane_smith",
    "role": "reader"
  }
]
```

#### POST `/topics/{topic_id}/users`
Add a user to a topic by nickname.

**Request:**
- Headers: `Authorization: Bearer <JWT_TOKEN>`
- Path: `topic_id` (UUID)
- Body:
```json
{
  "nickname": "john_doe",
  "role": "reader" // or "owner"
}
```

**Response (201):**
```json
{
  "message": "User added successfully"
}
```

**Access Rules:**
- Only topic owners can add users
- Cannot add users to system topics

#### DELETE `/topics/{topic_id}/users/{user_id}`
Remove a user from a topic.

**Request:**
- Headers: `Authorization: Bearer <JWT_TOKEN>`
- Path: `topic_id` (UUID), `user_id` (UUID)

**Response (204):** No content

**Access Rules:**
- Only topic owners can remove users
- Cannot remove the last owner

## Authentication

All endpoints require JWT authentication via the `Authorization` header:

```
Authorization: Bearer <JWT_TOKEN>
```

JWT token structure:
```json
{
  "sub": "123",           // User ID
  "nickname": "john_doe", // Username
  "role": "user",         // "user" or "system"
  "exp": 1700003600       // Expiration timestamp
}
```

**Note:** JWT validation is handled by the gateway. This service only extracts the token data.

## System Topics

Five system topics are pre-configured: `system_1`, `system_2`, `system_3`, `system_4`, `system_5`.

**Rules:**
- Visible to all users for reading
- Only users with `role="system"` can upload/delete documents
- User ID must match topic number (1-5)
- Cannot be deleted or modified

## Kafka Integration

### Producers

The service sends messages to Kafka topics:

- **PDF/PPTX/DOCX topics**: Document upload notifications
- **delete_documents**: Document deletion notifications
- **create_topic**: Topic creation notifications
- **delete_topic**: Topic deletion notifications

### Consumers

The service listens to Kafka topics:

- **document_status_updates**: Updates document status to "success"
- **new_users**: Creates new user records in the database

## MinIO Storage

Documents are stored in MinIO with the following structure:
- **Bucket name**: Topic ID (UUID)
- **Object name**: `{document_id}_{filename}`

Presigned URLs expire after the configured time (default: 1 hour).

## Environment Variables

See `.env.example` for all available configuration options.

Key variables:
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`
- `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`
- `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_*_TOPIC`
- `APP_HOST`, `APP_PORT`, `LOG_LEVEL`

## Database Migrations

Create a new migration:
```bash
alembic revision --autogenerate -m "description"
```

Apply migrations:
```bash
alembic upgrade head
```

Rollback:
```bash
alembic downgrade -1
```

## API Documentation

Interactive API documentation is available at:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## Error Codes

- `400 Bad Request`: Invalid input data
- `401 Unauthorized`: Missing or invalid JWT token
- `403 Forbidden`: Insufficient permissions
- `404 Not Found`: Resource not found
- `409 Conflict`: Resource conflict (e.g., duplicate)
- `422 Unprocessable Entity`: Validation error
- `500 Internal Server Error`: Server error

## Development

Run in development mode with auto-reload:
```bash
uvicorn app.main:app --reload
```

## License

[Your License Here]
