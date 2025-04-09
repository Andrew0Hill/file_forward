import asyncio
import logging
import os
import uuid
from asyncio import CancelledError
from typing import Literal

from fileforward.files import try_read_from_file, try_write_to_file
from fileforward.utils import cleanup_connection_files, memory_str

log = logging.getLogger(__name__)


async def _close_writer(writer: asyncio.StreamWriter, con_id: str = None):
    """ Helper function to cleanly close a StreamWriter.
    :param writer: The StreamWriter to close.
    :return: None
    """

    if writer.is_closing():
        log.debug(f"{con_id} Writer already closed.")
    else:
        log.debug(f"{con_id} Closing writer.")
        writer.close()
        await writer.wait_closed()


async def file_to_queue_task(queue: asyncio.Queue, file_path: str, polling_interval: int | float, con_id: str = None):
    """ Coroutine to read data from a file and write the data into a queue.
    :param queue: The queue instance to write data to.
    :param file_path: The file path to read from.
    :param polling_interval: How often to poll the file.
    :param con_id: The connection ID for this connection. Optional, only used for logging.
    :return: None
    """
    try:
        loop = asyncio.get_running_loop()
        # Last time we read from the file
        last_read = 0
        # Last known stats of the file.
        last_stats = None
        # How often to print stats?
        print_stat_interval = 30.0
        # When did we last print stats?
        first_stat_print = loop.time()
        last_stat_print = first_stat_print
        # How many read attempts?
        n_read_attempts = 0
        # How many successful reads?
        n_success_reads = 0
        # How much throughput?
        n_bytes_read = 0
        while True:
            cur_time = loop.time()
            # TODO: Move this to a function to clean up this loop
            time_since_last_stat_print = cur_time - last_stat_print
            if time_since_last_stat_print >= print_stat_interval:
                tput = n_bytes_read/(cur_time - first_stat_print)
                avg_read = n_bytes_read/n_success_reads if n_success_reads > 0 else 0
                log.info(f"{con_id} file_to_queue_task: Lifetime/Avg Reads {memory_str(n_bytes_read)}/{memory_str(avg_read)} | File Checks/Reads {n_success_reads}/{n_read_attempts} reads/checks. Avg. Throughput: {memory_str(tput)}/sec.")
                last_stat_print = cur_time
            # Check how long it has been since we last tried to read the file.
            time_since_last_read = loop.time() - last_read
            # If it has been less than the polling interval,
            if time_since_last_read <= polling_interval:
                time_to_sleep = polling_interval - time_since_last_read
                await asyncio.sleep(time_to_sleep)
            else:
                data, stats = await loop.run_in_executor(None, try_read_from_file, file_path, last_stats)
                last_read = loop.time()
                # Recording the stat_result from the previous read allows us to save time by skipping
                # the file read if it hasn't been changed.
                last_stats = stats
                n_read_attempts += 1
                # If we read nothing from the file, we don't have anything to put in the queue.
                if data is not None:
                    n_bytes_read += len(data)
                    n_success_reads += 1
                    await queue.put(data)
                else:
                    await asyncio.sleep(max(0, polling_interval - (loop.time() - last_read)))
    except CancelledError:
        log.info(f"{con_id} file_to_queue_task is cancelled.")


async def queue_to_file_task(queue: asyncio.Queue, file_path: str, con_id: str = None):
    """ Coroutine to read data from a queue and write the data into a file.
    :param queue: The queue instance to read from.
    :param file_path: The file path to write to.
    :param con_id: The connection ID for this connection. Optional, only used for logging.
    :return: None
    """
    try:
        loop = asyncio.get_running_loop()
        while True:
            data = await queue.get()
            await loop.run_in_executor(None, try_write_to_file, data, file_path)
    except CancelledError:
        log.info(f"{con_id} queue_to_file_task is cancelled.")


async def queue_to_writer_task(queue: asyncio.Queue, writer: asyncio.StreamWriter, con_id: str = None):
    """ Coroutine to read data from a queue and write the data to a StreamWriter
    :param queue: The asyncio.Queue instance to read from.
    :param writer: The asyncio.StreamWriter instance to write to.
    :param con_id: The connection ID for this connection. Optional, only used for logging.
    :return: None
    """
    try:
        while True:
            data = await queue.get()
            writer.write(data)
            await writer.drain()
    except CancelledError:
        log.info(f"{con_id} queue_to_writer_task is cancelled, closing writer.")
        await _close_writer(writer=writer, con_id=con_id)


async def reader_to_queue_task(queue: asyncio.Queue, reader: asyncio.StreamReader, n_bytes: int = int(1e6), con_id: str = None):
    """ Coroutine to read data from a StreamReader and write the data to a queue
    :param queue: The asyncio.Queue instance to write to.
    :param reader: The asyncio.StreamReader instance to read from.
    :param n_bytes: The maximum number of bytes to read from StreamReader at once.
    :param con_id: The connection ID for this connection. Optional, only used for logging.
    :return: None
    """
    try:
        while True:
            try:
                data = await reader.read(n_bytes)
            except ConnectionResetError:
                break
            # If we receive 0 bytes, connection is done.
            if len(data) == 0:
                break
            await queue.put(data)
    except CancelledError:
        log.info(f"{con_id} reader_to_queue_task is cancelled.")


async def file_forwarding_task(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        tunnel_dir: str,
        file_poll_int: int | float,
        caller: Literal["client", "server"],
        con_id: str = None
):
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
    :param file_poll_int: How often to perform I/O (i.e. call os.stat or open()) to check for file changes.
    :param caller: Used to route connections correctly depending on
    the function is being called from the client or server.
    :param con_id: The ID for this connection. Passing an ID when you are the server raises an error.
    :return: None
    """
    if caller == "server":
        if con_id is not None:
            raise RuntimeError("Should not pass a value for con_id when running as the server!")
        # Generate a random, unique ID for this connection.
        con_id = str(uuid.uuid4())
        log.info(f"Creating new server connection for {con_id}")

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

    # TODO: Can we use a TaskGroup here?
    # List to hold the tasks.
    all_tasks = [
        # Task 1a
        asyncio.create_task(reader_to_queue_task(queue=outgoing_queue, reader=reader, con_id=con_id), name=f"{con_id}_read_to_queue"),
        # Task 1b
        asyncio.create_task(queue_to_file_task(queue=outgoing_queue, file_path=outgoing_fpath, con_id=con_id), name=f"{con_id}_queue_to_file"),
        # Task 2a
        asyncio.create_task(file_to_queue_task(queue=incoming_queue, file_path=incoming_fpath, polling_interval=file_poll_int, con_id=con_id), name=f"{con_id}_file_to_queue"),
        # Task 2b
        asyncio.create_task(queue_to_writer_task(queue=incoming_queue, writer=writer, con_id=con_id), name=f"{con_id}_queue_to_write")
    ]

    try:
        # We will reach this point when the first task encounters an issue or completes.
        completed, pending = await asyncio.wait(all_tasks, return_when=asyncio.FIRST_COMPLETED)

        # If we make it here, the task completed 'successfully'.
        # Only the reader_to_queue has a true completion path (if we read 0 bytes from socket).
        # The other tasks will need to be cancelled or encounter some other exception to exit.
        log.debug(f"{con_id} Sending task cancellations for pending tasks.")
        await _cancel_tasks(pending, con_id)
    except CancelledError:
        log.warning(f"{con_id} file_forwarding_task received cancellation.")
        await _cancel_tasks(task_l=all_tasks, con_id=con_id)
    finally:
        # If we are the server, we should also clean up the files created by this connection.
        if caller == "server":
            log.info(f"{con_id} Attempting to clean up connection files.")
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, cleanup_connection_files, tunnel_dir, con_id)
            log.info(f"{con_id} Cleaned up connection files successfully.")


async def _cancel_tasks(task_l: list | set, con_id: str):
    for task in task_l:
        if not task.done():
            task.cancel()
    log.debug(f"{con_id} Waiting for pending tasks to exit.")
    await asyncio.gather(*task_l, return_exceptions=True)
    log.debug(f"{con_id} Pending tasks completed.")
