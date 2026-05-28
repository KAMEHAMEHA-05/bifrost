from dataclasses import dataclass
from typing import Any
import time
import uuid


@dataclass
class Message:

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