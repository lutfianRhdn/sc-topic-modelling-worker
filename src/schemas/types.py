import strawberry
from typing import List

@strawberry.type
class TopicDocument:
    full_text: str
    topic: str
    tweet_url: str
    username: str

@strawberry.type
class TopicDocResponse:
    data: List[TopicDocument]
    message: str
    status: int

@strawberry.type
class TopicProject:
    context: str
    keyword: str
    project_id: str = strawberry.field(name="projectId")
    topic_id: int = strawberry.field(name="topicId")
    words: List[str]

@strawberry.type
class TopicResponse:
    data: List[TopicProject]
    message: str
    status: int