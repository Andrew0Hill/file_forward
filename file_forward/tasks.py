import asyncio
import time
import uuid
import os
from typing import Literal

from . import files


async def _close_writer(writer: asyncio.StreamWriter):
    """ Helper function to cleanly close a StreamWriter.
    :param writer:
    :return:
    """
    if not writer.is_closing():
        writer.close()
        await writer.wait_closed()


async def file_to_queue_task(queue: asyncio.Queue, file_path: str, polling_interval: float = 0.01):
    """ Coroutine to read data from a file and write the data into a queue.
    :param queue: The queue instance to write data to.
    :param file_path: The file path to read from.
    :param polling_interval: How often to poll the file.
    :return: None
    """
    loop = asyncio.get_running_loop()

    last_read = 0
    while True:
        time_since_last_read = time.perf_counter() - last_read
        if time_since_last_read <= polling_interval:
            time_to_sleep = polling_interval - time_since_last_read
            #print(f"Sleeping for {time_to_sleep:0.3f} to avoid polling too quickly.")
            await asyncio.sleep(time_to_sleep)
        else:
            #print("Polling file.")
            data = await loop.run_in_executor(None, files.try_read_from_file, file_path)
            last_read = time.perf_counter()
            if data != b"":
                await queue.put(data)
            else:
                await asyncio.sleep(0)


async def queue_to_file_task(queue: asyncio.Queue, file_path: str):
    """ Coroutine to read data from a queue and write the data into a file.
    :param queue: The queue instance to read from.
    :param file_path: The file path to write to.
    :return: None
    """
    loop = asyncio.get_running_loop()
    while True:
        data = await queue.get()
        await loop.run_in_executor(None, files.try_write_to_file, data, file_path)


async def queue_to_writer_task(queue: asyncio.Queue, writer: asyncio.StreamWriter):
    """ Coroutine to read data from a queue and write the data to a StreamWriter
    :param queue: The asyncio.Queue instance to read from.
    :param writer: The asyncio.StreamWriter instance to write to.
    :return: None
    """
    while True:
        data = await queue.get()
        writer.write(data)
        await writer.drain()


async def reader_to_queue_task(queue: asyncio.Queue, reader: asyncio.StreamReader, n_bytes: int = 4096):
    """ Coroutine to read data from a StreamReader and write the data to a queue
    :param queue: The asyncio.Queue instance to write to.
    :param reader: The asyncio.StreamReader instance to read from.
    :return: None
    """
    while True:
        try:
            data = await reader.read(n_bytes)
        except ConnectionResetError:
            break
        # If we receive 0 bytes, connection is done/
        if len(data) == 0:
            break
        await queue.put(data)


async def file_forwarding_task(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, tunnel_dir: str, caller: Literal["client", "server"], con_id: str = None):
    """ Function accepting a StreamReader and StreamWriter
    sets up the required coroutines to send and receive information via file
    forwarding.

    The general idea is this:

    Connections to the Local server are forwarded to the Remote Client:
        Server -> Data Queue (local) -> Forwarding File (local) -> Client
    Responses from Remote Client are forwarded to Local Server:
        Server <- Data Queue (remote) <- Forwarding File (remote) <- Client


    :param reader: StreamReader instance for the current connection.
    :param writer: StreamWriter instance for the current connection.
    :param tunnel_dir: The directory which will hold the connection files.
    :param caller: Used to route connections correctly depending on
    the function is being called from the client or server.
    :param con_id: The ID for this connection. Passing an ID when you are the server raises an error.
    :return: None
    """
    if (caller == "server") and (con_id is not None):
        raise RuntimeError("Should not pass a value for con_id when running as the server!")

    # Generate a random, unique ID for this connection if it is not provided.
    con_id = str(uuid.uuid4()) if con_id is None else con_id

    # To implement the above connections, we need to initialize four (4) coroutines/tasks
    # Task 1a - StreamReader -> Data Queue (local)
    # Task 1b - Data Queue (local) -> Forwarding File (local)
    # Task 2a - Forwarding File (remote) -> Data Queue (remote)
    # Task 2b - Data Queue (remote) -> StreamWriter

    # Create the two required queues
    outgoing_queue = asyncio.Queue()
    incoming_queue = asyncio.Queue()

    # Create the two file paths
    if caller == "server":
        incoming_suffix = "remote"
        outgoing_suffix = "local"
    elif caller == "client":
        incoming_suffix = "local"
        outgoing_suffix = "remote"
    else:
        raise RuntimeError(f"Invalid value '{caller}' for caller.")

    incoming_fpath = os.path.join(tunnel_dir, f"{con_id}.{incoming_suffix}")
    outgoing_fpath = os.path.join(tunnel_dir, f"{con_id}.{outgoing_suffix}")

    # List to hold the tasks.
    all_tasks = [
        # Task 1a
        asyncio.create_task(reader_to_queue_task(queue=outgoing_queue, reader=reader),
                            name=f"{con_id}_read_to_queue"),
        # Task 1b
        asyncio.create_task(queue_to_file_task(queue=outgoing_queue, file_path=outgoing_fpath),
                            name=f"{con_id}_queue_to_file"),
        # Task 2a
        asyncio.create_task(file_to_queue_task(queue=incoming_queue, file_path=incoming_fpath),
                            name=f"{con_id}_file_to_queue"),
        # Task 2b
        asyncio.create_task(queue_to_writer_task(queue=incoming_queue, writer=writer),
                            name=f"{con_id}_queue_to_write")
    ]

    # We will reach this point when the first task encounters an issue or completes.
    completed, pending = await asyncio.wait(all_tasks, return_when=asyncio.FIRST_COMPLETED)
    # At this point, we will cancel all remaining tasks.
    print("Cancelling tasks.")
    for pend_t in pending:
        pend_t.cancel()
    # If we are the server, we should also clean up the files created by this connection.
    if caller == "server":
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, cleanup_connection_files_task, tunnel_dir, con_id)
    # Finally, close the writer down cleanly.
    await _close_writer(writer)
    # This file forwarding connection is now complete.

