# GraphQL Worker Usage Examples

This document provides examples of how to use the GraphQL Worker.

## Starting the Worker

The GraphQL Worker is automatically started by the Supervisor when you run the application. It will be available on port `REST_API_PORT + 1` (default: 5001).

## Example Queries

### 1. Get Topics by Project ID

**GraphQL Query:**
```graphql
{
  topicByProject(projectId: "your-project-id") {
    data {
      context
      keyword
      projectId
      topicId
      words
    }
    message
    status
  }
}
```

**cURL Example:**
```bash
curl -X POST http://localhost:5001/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "{ topicByProject(projectId: \"your-project-id\") { data { context keyword projectId topicId words } message status } }"
  }'
```

**Using Variables:**
```bash
curl -X POST http://localhost:5001/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query GetTopics($projectId: String!) { topicByProject(projectId: $projectId) { data { context keyword projectId topicId words } message status } }",
    "variables": { "projectId": "your-project-id" }
  }'
```

### 2. Get Documents by Project ID

**GraphQL Query:**
```graphql
{
  documentsByProject(projectId: "your-project-id") {
    data {
      fullText
      topic
      tweetUrl
      username
    }
    message
    status
  }
}
```

**cURL Example:**
```bash
curl -X POST http://localhost:5001/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "{ documentsByProject(projectId: \"your-project-id\") { data { fullText topic tweetUrl username } message status } }"
  }'
```

### 3. Combined Query (Topics and Documents)

```graphql
{
  topics: topicByProject(projectId: "your-project-id") {
    data {
      context
      keyword
      projectId
      topicId
      words
    }
    message
    status
  }
  documents: documentsByProject(projectId: "your-project-id") {
    data {
      fullText
      topic
      tweetUrl
      username
    }
    message
    status
  }
}
```

## Response Examples

### Successful Topic Response
```json
{
  "data": {
    "topicByProject": {
      "data": [
        {
          "context": "Technology trends",
          "keyword": "AI",
          "projectId": "your-project-id",
          "topicId": 1,
          "words": ["artificial", "intelligence", "machine", "learning"]
        }
      ],
      "message": "Success",
      "status": 200
    }
  }
}
```

### Error Response
```json
{
  "data": {
    "topicByProject": null
  },
  "errors": [
    {
      "message": "projectId is required"
    }
  ]
}
```

## Health Check

Check if the GraphQL Worker is running:

```bash
curl http://localhost:5001/
```

Response:
```json
{
  "message": "Welcome to the GraphQL Worker",
  "status": "success",
  "graphql_endpoint": "/graphql",
  "federation": "enabled"
}
```

## Integration with REST API

The GraphQL Worker runs alongside the REST API Worker, providing the same functionality through different interfaces:

- **REST API**: `http://localhost:5000/topic-by-project/{projectId}`
- **GraphQL**: `http://localhost:5001/graphql` with `topicByProject(projectId: "...")`

Both endpoints access the same underlying data and caching mechanisms.

## Federation 2 Support

The GraphQL Worker is built with Apollo Federation 2 support, allowing it to be composed with other GraphQL services. The schema can be extended and federated as needed for microservice architectures.