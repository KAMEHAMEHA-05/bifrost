from dataclasses import dataclass
from typing import Any
import time
import uuid
from pydantic import BaseModel


@dataclass
class Message():

    id: str

    topic: str

    payload: Any

    timestamp: float


def create_message(

    topic,
    payload
):

    return Message(

        id=str(uuid.uuid4()),

        topic=topic,

        payload=payload,

        timestamp=time.time()
    )


class TopicRequest(BaseModel):

    name: str

    maxsize: int = 0


class PublishRequest(BaseModel):

    payload: Any