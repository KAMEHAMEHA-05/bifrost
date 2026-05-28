from fastapi import FastAPI
from fastapi import HTTPException

from memory import (
    InMemoryBuffer
)

from models import (
    PublishRequest,
    TopicRequest
)

from exceptions import (
    TopicNotFound
)

app = FastAPI(
    title="Torrential Bifrost"
)

buffer = InMemoryBuffer()


@app.get("/health")
async def health():

    return {
        "status": "ok"
    }


@app.post("/topics/create")
async def create_topic(
    request: TopicRequest
):

    await buffer.create_topic(

        topic=request.name,

        maxsize=request.maxsize
    )

    return {

        "success": True,

        "topic": request.name
    }


@app.post("/topics/{topic}/publish")
async def publish(
    topic: str,
    request: PublishRequest
):

    try:

        await buffer.publish(

            topic=topic,

            payload=request.payload
        )

        return {

            "success": True,

            "topic": topic
        }

    except TopicNotFound:

        raise HTTPException(

            status_code=404,

            detail=(
                f"Topic '{topic}' "
                f"does not exist"
            )
        )


@app.post("/topics/{topic}/consume")
async def consume(
    topic: str
):

    try:

        consumer = (
            buffer.consume(topic)
        )

        message = await anext(
            consumer
        )

        return {

            "success": True,

            "message": {

                "id":
                    message.id,

                "topic":
                    message.topic,

                "payload":
                    message.payload,

                "timestamp":
                    message.timestamp
            }
        }

    except TopicNotFound:

        raise HTTPException(

            status_code=404,

            detail=(
                f"Topic '{topic}' "
                f"does not exist"
            )
        )


@app.get("/topics")
async def list_topics():

    return {

        "topics": list(
            buffer.topics.keys()
        )
    }