import asyncio

from collections import defaultdict

from base import BufferBackend

from models import create_message

from exceptions import (
    TopicNotFound
)


class Topic:

    def __init__(
        self,
        name,
        maxsize=0
    ):

        self.name = name

        self.maxsize = maxsize

        self.messages_published = 0

        self.queue = asyncio.Queue(
            maxsize=maxsize
        )


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
        if self.closed:
            raise RuntimeError(
                "Buffer closed"
            )

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
        if self.closed:
            raise RuntimeError(
                "Buffer closed"
            )

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

        if self.closed:

            raise RuntimeError(
                "Buffer closed"
            )

        if topic not in self.topics:

            raise TopicNotFound(topic)

        msg = create_message(

            topic,

            payload
        )

        self.topics[
            topic
        ].messages_published += 1

        await self.topics[
            topic
        ].queue.put(msg)


    async def consume(

        self,

        topic
    ):

        if self.closed:

            raise RuntimeError(
                "Buffer closed"
            )

        if topic not in self.topics:

            raise TopicNotFound(topic)

        while True:

            msg = await self.topics[
                topic
            ].queue.get()

            yield msg

    async def shutdown(self):

        self.closed = True

        self.topics.clear()


    
    async def unsubscribe(

        self,

        topic,

        queue
    ):

        if topic not in self.topics:

            raise TopicNotFound(topic)

        subscribers = self.topics[
            topic
        ].subscribers

        if queue in subscribers:

            subscribers.remove(queue)


    async def list_topics(self):

        return list(
            self.topics.keys()
        )
    

    async def topic_stats(

        self,

        topic
    ):

        if topic not in self.topics:

            raise TopicNotFound(topic)

        t = self.topics[topic]

        return {

            "name": t.name,

            "subscribers": len(
                t.subscribers
            ),

            "messages_published":
                t.messages_published,

            "maxsize":
                t.maxsize
        }