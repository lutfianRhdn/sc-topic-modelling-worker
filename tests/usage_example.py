#!/usr/bin/env python3
"""
Example usage demonstration for the updated get_document_topic_by_project function.

This shows how to use the new topic filtering parameter in GraphQL queries.
"""

def demonstrate_usage():
    """Demonstrate the usage of the new topic parameter"""
    print("=" * 70)
    print("GraphQL Query Examples for get_document_topic_by_project")
    print("=" * 70)
    
    print("\n1. Query WITHOUT topic filter (original behavior):")
    query_without_topic = '''
    {
      getDocumentTopicByProject(projectId: "project123") {
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
    '''
    print(query_without_topic)
    
    print("\n2. Query WITH topic filter (new functionality):")
    query_with_topic = '''
    {
      getDocumentTopicByProject(projectId: "project123", topic: "politics") {
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
    '''
    print(query_with_topic)
    
    print("\n3. Using variables in GraphQL:")
    query_with_variables = '''
    query GetDocumentsByProjectAndTopic($projectId: String!, $topic: String) {
      getDocumentTopicByProject(projectId: $projectId, topic: $topic) {
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
    '''
    print(query_with_variables)
    
    variables_example = '''
    Variables:
    {
      "projectId": "project123",
      "topic": "sports"
    }
    '''
    print(variables_example)
    
    print("\n" + "=" * 70)
    print("Implementation Details")
    print("=" * 70)
    
    print("""
Cache Key Behavior:
- Without topic: cache_key = "doc_project123"
- With topic: cache_key = "doc_project123_topic_politics"

This ensures proper cache separation for filtered and unfiltered results.

Filtering Logic:
- If topic is None or not provided: Returns all documents
- If topic is provided: Returns only documents where doc.topic == topic

Performance Notes:
- Filtering happens in-memory after data retrieval
- Separate cache entries for different topic filters
- Original caching behavior preserved for backward compatibility
    """)
    
    print("\n✅ The topic filtering functionality has been successfully implemented!")

if __name__ == '__main__':
    demonstrate_usage()