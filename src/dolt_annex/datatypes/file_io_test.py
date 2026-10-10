import asyncio
from dataclasses import dataclass
from typing_extensions import Literal
import pytest

from dolt_annex.datatypes.file_io import BytesQueue

@pytest.mark.asyncio
async def test_bytes_queue():

    @dataclass
    class Event:
        action: Literal["insert", "consume"]
        contents: bytes
        new_size: int
    
    queue = BytesQueue(maxsize=10)
    events: list[Event] = []

    async def insert_task(data: bytes):
        await queue.put(data)
        events.append(Event(action="insert", contents=data, new_size=queue.qsize()))

    async def consume_task():
        data = await queue.get()
        events.append(Event(action="consume", contents=data, new_size=queue.qsize()))

    async def producer():
        await insert_task(b"task 1")
        await insert_task(b"task 2")
        await insert_task(b"task 3")

    async def consumer():
        await consume_task()
        await consume_task()
        await consume_task()

    """
    Because the tasks are run eagerly, we can exactly predict the exact order of events:
    Each task will run until it is blocked.
    The producer will be blocked once the queue exceeds its maximum size.
    The consumer will be blocked if the queue is empty.
    Thus, we predict that the tasks will alternate completely filling and completely exhausting the queue.
    """
    expected_events: list[Event] = [
        Event(action="insert", contents=b"task 1", new_size=6),
        Event(action="insert", contents=b"task 2", new_size=12),
        Event(action="consume", contents=b"task 1", new_size=6),
        Event(action="consume", contents=b"task 2", new_size=0),
        Event(action="insert", contents=b"task 3", new_size=6),
        Event(action="consume", contents=b"task 3", new_size=0),
    ]
    
    async with asyncio.TaskGroup() as tg:
        tg.create_task(producer(), eager_start=True)
        tg.create_task(consumer(), eager_start=True)

    assert events == expected_events