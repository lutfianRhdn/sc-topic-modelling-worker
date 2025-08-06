import strawberry
from typing import Optional
from .types import TopicResponse, TopicDocResponse, TopicDocument, TopicProject
import uuid
import threading

@strawberry.type
class Query:
    """GraphQL Query root type"""
    
    @strawberry.field
    def get_topic_by_project(self, projectId: str, info: strawberry.Info) -> TopicResponse:
        """Get topics by project ID"""
        # Get the GraphQLWorker instance from context
        worker = info.context.get("worker")
        if not worker:
            return TopicResponse(
                data=[],
                message="Worker context not available",
                status=500
            )
        
        # Use the same logic as RestApiWorker
        result = worker.send_to_other_worker(
            destination=[f"CacheWorker/getByKey/topic_{projectId}"],
            data={"projectId": f"topic_{projectId}"}
        )
        
        cache_result = result.get("result")
        if cache_result is None or len(cache_result) == 0:
            # Get from database if not in cache
            result = worker.send_to_other_worker(
                destination=[f"DatabaseInteractionWorker/getTopicByProjectId/{projectId}"],
                data={}
            )
            cache_result = result['result']
            
            # Cache the result
            worker.send_message_async(
                destination=[f'CacheWorker/set/topic_{projectId}'],
                data={
                    "key": f"topic_{projectId}",
                    "value": cache_result,
                }
            )
        print(f"Cache result for topic_{projectId}: {cache_result}")
        
        # Convert response to GraphQL format
        if not cache_result or cache_result.get("status") != 'completed':
            return TopicResponse(
                data=[],
                message=cache_result.get("message", "Failed to retrieve topics") if cache_result else "No data found",
                status=cache_result.get("status", 404) if cache_result else 404
            )
        
        # Convert data to TopicProject objects
        topic_data = cache_result['result']
        if isinstance(topic_data, list):
            projects = [
                TopicProject(
                    context=item.get("context", ""),
                    keyword=item.get("keyword", ""),
                    project_id=item.get("projectId", ""),
                    topic_id=item.get("topicId", 0),
                    words=item.get("words", [])
                )
                for item in topic_data
                if isinstance(item, dict)
            ]
        else:
            projects = []
        
        return TopicResponse(
            data=projects,
            message=cache_result.get("message", "Success"),
            status=cache_result.get("status", 200)
        )
    
    @strawberry.field
    def get_document_topic_by_project(self, projectId: str, info: strawberry.Info) -> TopicDocResponse:
        """Get documents by project ID"""
        # Get the GraphQLWorker instance from context
        worker = info.context.get("worker")
        if not worker:
            return TopicDocResponse(
                data=[],
                message="Worker context not available",
                status=500
            )
        
        # Use the same logic as RestApiWorker
        result = worker.send_to_other_worker(
            destination=[f"CacheWorker/getByKey/doc_{projectId}"],
            data={"projectId": f"doc_{projectId}"}
        )
        
        cache_result = result['result']
        if cache_result is None or len(cache_result) == 0:
            # Get from database if not in cache
            result = worker.send_to_other_worker(
                destination=[f"DatabaseInteractionWorker/getDocumentsByProjectId/{projectId}"],
                data={}
            )
            cache_result = result.get("result")
            
            # Cache the result
            worker.send_message_async(
                destination=[f'CacheWorker/set/doc_{projectId}'],
                data={
                    "key": f"doc_{projectId}",
                    "value": cache_result,
                }
            )
        # Convert response to GraphQL format
        if not cache_result or result['status'] != 'completed':
            return TopicDocResponse(
                data=[],
                message=cache_result.get("message", "Failed to retrieve documents") if cache_result else "No data found",
                status=cache_result.get("status", 404) if cache_result else 404
            )
        
        # Convert data to TopicDocument objects
        doc_data = cache_result
        print(f"Cache result for doc_{projectId}: {doc_data}")
        if isinstance(doc_data, list):
            documents = [
                TopicDocument(
                    full_text=item.get("full_text", ""),
                    topic=item.get("topic", ""),
                    tweet_url=item.get("tweet_url", ""),
                    username=item.get("username", "")
                )
                for item in doc_data
                if isinstance(item, dict)
            ]
        else:
            documents = []
        
        return TopicDocResponse(
            data=documents,
            message="success",
            status=200
        )