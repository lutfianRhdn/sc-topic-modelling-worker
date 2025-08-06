# GraphQL Schema Documentation

This GraphQL Worker provides a GraphQL interface similar to the REST API Worker but with federation support.

## Endpoints

### Health Check
- **URL**: `GET /`
- **Response**: Returns health status and GraphQL endpoint information

### GraphQL Endpoint
- **URL**: `POST /graphql`
- **Content-Type**: `application/json`

## GraphQL Schema

### Queries

#### topicByProject
Get topics by project ID.

**Query:**
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

**Variables:**
```json
{
  "projectId": "your-project-id"
}
```

#### documentsByProject
Get documents by project ID.

**Query:**
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

## Types

### TopicProject
- `context: String!` - Topic context
- `keyword: String!` - Topic keyword
- `projectId: String!` - Project identifier
- `topicId: Int!` - Topic identifier
- `words: [String!]!` - List of topic words

### TopicResponse
- `data: [TopicProject!]!` - List of topic projects
- `message: String!` - Response message
- `status: Int!` - HTTP status code

### TopicDocument
- `fullText: String!` - Full text of the document
- `topic: String!` - Topic classification
- `tweetUrl: String!` - URL to the tweet
- `username: String!` - Username of the author

### TopicDocResponse
- `data: [TopicDocument!]!` - List of topic documents
- `message: String!` - Response message
- `status: Int!` - HTTP status code

## Federation 2 Support

This GraphQL worker is built with Federation 2 support enabled, allowing it to be composed with other GraphQL services in a federated graph.

## Worker Communication

The GraphQL Worker communicates with other workers in the system using the same inter-process communication pattern as the REST API Worker:

1. **CacheWorker** - For caching responses
2. **DatabaseInteractionWorker** - For database operations
3. **Other workers** - As needed for processing

## Configuration

The GraphQL Worker runs on port `REST_API_PORT + 1` by default and is managed by the Supervisor alongside other workers.