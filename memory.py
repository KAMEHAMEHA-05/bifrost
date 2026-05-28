import asyncio

from collections import defaultdict

from .base import BufferBackend

from .models import create_message

from .exceptions import (
    TopicNotFound
)


class Topic:

    def __init__(
        self,
        name,
        maxsize=0
    ):

        self.name = name

        self.subscribers = []

        self.maxsize = maxsize


class InMemoryBuffer(

    BufferBackend
):

    def __init__(self):

        self.topics = {}

        self.closed = False


    async def create_topic(

        self,

        topic,

        maxsize=0
    ):

        if topic in self.topics:
            return

        self.topics[topic] = Topic(

            name=topic,

            maxsize=maxsize
        )


    async def subscribe(

        self,

        topic
    ):

        if topic not in self.topics:

            raise TopicNotFound(topic)

        queue = asyncio.Queue(

            maxsize=self.topics[
                topic
            ].maxsize
        )

        self.topics[
            topic
        ].subscribers.append(
            queue
        )

        return queue


    async def publish(

        self,

        topic,

        payload
    ):

        if topic not in self.topics:

            raise TopicNotFound(topic)

        msg = create_message(

            topic,

            payload
        )

        subscribers = (
            self.topics[
                topic
            ].subscribers
        )

        for queue in subscribers:

            await queue.put(msg)


    async def consume(

        self,

        topic
    ):

        queue = await self.subscribe(
            topic
        )

        while True:

            msg = await queue.get()

            yield msg


    async def shutdown(self):

        self.closed = True

        self.topics.clear()