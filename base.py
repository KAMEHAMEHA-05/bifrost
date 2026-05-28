from abc import ABC, abstractmethod


class BufferBackend(ABC):

    @abstractmethod
    async def create_topic(
        self,
        topic,
        maxsize=0
    ):
        pass


    @abstractmethod
    async def publish(
        self,
        topic,
        payload
    ):
        pass


    @abstractmethod
    async def consume(
        self,
        topic
    ):
        pass


    @abstractmethod
    async def subscribe(
        self,
        topic
    ):
        pass


    @abstractmethod
    async def shutdown(self):
        pass