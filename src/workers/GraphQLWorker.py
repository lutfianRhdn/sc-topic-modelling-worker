from multiprocessing.connection import Connection
from flask import Flask, request, jsonify
import threading
import uuid
import asyncio
import time
import json
import utils.log as log
from utils.handleMessage import sendMessage, convertMessage

# Simple GraphQL-like implementation without external dependencies
app = Flask(__name__)

class GraphQLWorker:
    ###############
    # dont edit this part
    ###############
    conn: Connection
    requests: dict = {}
    
    def run(self, conn: Connection, port: int):
        # assign here
        GraphQLWorker.conn = conn
        self._port = port
        
        # Register endpoints
        app.add_url_rule('/graphql', 'graphql', self.graphql_endpoint, methods=['POST'])
        app.add_url_rule('/', 'health', self.health_check, methods=['GET'])
        
        # Start background listener task
        def run_listen_task():
            asyncio.run(self.listen_task())
        threading.Thread(target=run_listen_task, daemon=True).start()
        
        # Start Flask server
        app.run(debug=True, port=self._port, use_reloader=False, host="0.0.0.0")
    
    def health_check(self):
        return jsonify({
            "message": "Welcome to the GraphQL Worker",
            "status": "success",
            "graphql_endpoint": "/graphql",
            "federation": "enabled"
        }), 200
    
    def graphql_endpoint(self):
        """Handle GraphQL queries"""
        try:
            data = request.get_json()
            query = data.get('query', '')
            variables = data.get('variables', {})
            
            # Parse the query and route to appropriate resolver
            result = self.execute_query(query, variables)
            
            return jsonify(result), 200
            
        except Exception as e:
            log.log(f"GraphQL error: {e}", 'error')
            return jsonify({
                "errors": [{"message": str(e)}]
            }), 500
    
    def execute_query(self, query: str, variables: dict):
        """Simple GraphQL query executor"""
        # Extract operation from query
        query = query.strip()
        
        if 'topicByProject' in query:
            # Extract project_id from variables or query
            project_id = variables.get('projectId') or self.extract_project_id_from_query(query)
            return self.resolve_topic_by_project(project_id)
        
        elif 'documentsByProject' in query:
            # Extract project_id from variables or query
            project_id = variables.get('projectId') or self.extract_project_id_from_query(query)
            return self.resolve_documents_by_project(project_id)
        
        else:
            return {
                "data": None,
                "errors": [{"message": "Unknown query operation"}]
            }
    
    def extract_project_id_from_query(self, query: str):
        """Extract projectId from query string"""
        import re
        # Look for patterns like: topicByProject(projectId: "value")
        match = re.search(r'projectId:\s*"([^"]+)"', query)
        if match:
            return match.group(1)
        return None
    
    def resolve_topic_by_project(self, project_id: str):
        """Resolve topicByProject query"""
        if not project_id:
            return {
                "data": {"topicByProject": None},
                "errors": [{"message": "projectId is required"}]
            }
        
        try:
            # Use the same logic as RestApiWorker
            result = self.send_to_other_worker(
                destination=[f"CacheWorker/getByKey/topic_{project_id}"],
                data={"project_id": f"topic_{project_id}"}
            )
            
            cache_result = result.get("result")
            if cache_result is None or len(cache_result) == 0:
                # Get from database if not in cache
                result = self.send_to_other_worker(
                    destination=[f"DatabaseInteractionWorker/getTopicByProjectId/{project_id}"],
                    data={}
                )
                cache_result = result.get("result")
                
                # Cache the result
                self.send_message_async(
                    destination=[f'CacheWorker/set/topic_{project_id}'],
                    data={
                        "key": f"topic_{project_id}",
                        "value": cache_result,
                    }
                )
            
            # Convert response to GraphQL format
            if not cache_result or cache_result.get("status") != 200:
                return {
                    "data": {
                        "topicByProject": {
                            "data": [],
                            "message": cache_result.get("message", "Failed to retrieve topics") if cache_result else "No data found",
                            "status": cache_result.get("status", 404) if cache_result else 404
                        }
                    }
                }
            
            # Format data for GraphQL response
            topic_data = cache_result.get("data", [])
            formatted_data = []
            if isinstance(topic_data, list):
                for item in topic_data:
                    if isinstance(item, dict):
                        formatted_data.append({
                            "context": item.get("context", ""),
                            "keyword": item.get("keyword", ""),
                            "projectId": item.get("projectId", ""),
                            "topicId": item.get("topicId", 0),
                            "words": item.get("words", [])
                        })
            
            return {
                "data": {
                    "topicByProject": {
                        "data": formatted_data,
                        "message": cache_result.get("message", "Success"),
                        "status": cache_result.get("status", 200)
                    }
                }
            }
            
        except Exception as e:
            log.log(f"Error in topicByProject resolver: {e}", 'error')
            return {
                "data": {"topicByProject": None},
                "errors": [{"message": str(e)}]
            }
    
    def resolve_documents_by_project(self, project_id: str):
        """Resolve documentsByProject query"""
        if not project_id:
            return {
                "data": {"documentsByProject": None},
                "errors": [{"message": "projectId is required"}]
            }
        
        try:
            # Use the same logic as RestApiWorker
            result = self.send_to_other_worker(
                destination=[f"CacheWorker/getByKey/doc_{project_id}"],
                data={"project_id": f"doc_{project_id}"}
            )
            
            cache_result = result.get("result")
            if cache_result is None or len(cache_result) == 0:
                # Get from database if not in cache
                result = self.send_to_other_worker(
                    destination=[f"DatabaseInteractionWorker/getDocumentsByProjectId/{project_id}"],
                    data={}
                )
                cache_result = result.get("result")
                
                # Cache the result
                self.send_message_async(
                    destination=[f'CacheWorker/set/doc_{project_id}'],
                    data={
                        "key": f"doc_{project_id}",
                        "value": cache_result,
                    }
                )
            
            # Convert response to GraphQL format
            if not cache_result or cache_result.get("status") != 200:
                return {
                    "data": {
                        "documentsByProject": {
                            "data": [],
                            "message": cache_result.get("message", "Failed to retrieve documents") if cache_result else "No data found",
                            "status": cache_result.get("status", 404) if cache_result else 404
                        }
                    }
                }
            
            # Format data for GraphQL response
            doc_data = cache_result.get("data", [])
            formatted_data = []
            if isinstance(doc_data, list):
                for item in doc_data:
                    if isinstance(item, dict):
                        formatted_data.append({
                            "fullText": item.get("full_text", ""),
                            "topic": item.get("topic", ""),
                            "tweetUrl": item.get("tweet_url", ""),
                            "username": item.get("username", "")
                        })
            
            return {
                "data": {
                    "documentsByProject": {
                        "data": formatted_data,
                        "message": cache_result.get("message", "Success"),
                        "status": cache_result.get("status", 200)
                    }
                }
            }
            
        except Exception as e:
            log.log(f"Error in documentsByProject resolver: {e}", 'error')
            return {
                "data": {"documentsByProject": None},
                "errors": [{"message": str(e)}]
            }
    
    async def listen_task(self):
        while True:
            try:
                if GraphQLWorker.conn.poll(1):  # Check for messages with 1 second timeout
                    print("Listening for messages...")
                    
                    raw = GraphQLWorker.conn.recv()
                    print(f"Received raw message: {raw}")
                    msg = convertMessage(raw)
                    self.on_processed(raw)
                    await asyncio.sleep(0.1)  # Yield control to the event loop
            except EOFError:
                break
            except Exception as e:
                log.log(f"Listener error: {e}", 'error')
                break
    
    def on_processed(self, msg: dict):
        """
        Called when a worker response comes in.
        msg must contain 'messageId' and 'data'.
        """
        task_id = msg.get("messageId")
        print(f"Received response for task_id: {task_id} with data: {msg.get('data')}")
        
        entry = GraphQLWorker.requests.get(task_id)
        if not entry:
            return
        entry["response"] = msg.get("data")
        entry["event"].set()
    
    def send_to_other_worker(self, destination: list, data: dict):
        """Send synchronous message to other worker and wait for response"""
        task_id = str(uuid.uuid4())
        evt = threading.Event()
        
        GraphQLWorker.requests[task_id] = {
            "event": evt,
            "response": None
        }
        print(f"Sending request to {destination} with task_id: {task_id}")
        
        sendMessage(
            conn=GraphQLWorker.conn,
            messageId=task_id,
            status="processing",
            destination=destination,
            data=data
        )
        
        if not evt.wait(timeout=10):
            # timeout
            return {
                "taskId": task_id,
                "status": "timeout",
                "result": None
            }
        
        # success
        result = GraphQLWorker.requests.pop(task_id)["response"]
        return {
            "taskId": task_id,
            "status": "completed",
            "result": result
        }
    
    def send_message_async(self, destination: list, data: dict):
        """Send asynchronous message to other worker (fire and forget)"""
        task_id = str(uuid.uuid4())
        
        sendMessage(
            conn=GraphQLWorker.conn,
            messageId=task_id,
            status="processing",
            destination=destination,
            data=data
        )

def main(conn: Connection, config: dict):
    worker = GraphQLWorker()
    worker.run(conn, config.get("port", 8000))