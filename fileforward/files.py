import fcntl
import logging
import os
import time
import math
import random
from typing import Literal

log = logging.getLogger(__name__)


def acquire_file_lock(f, max_retries: int = 11):
    """ This function attempts to acquire an exclusive lockf() lock
    on the target file.

    If the first lock attempt fails, we block and retry with a binary
    exponential backoff.

    The delays are in ms, so 1st retry waits randomly [0, 2]ms, the
    second between [0,4]ms, etc.

    This function is synchronous and blocks for I/O and to sleep
    if the lock is not available, so should only be called
    from a non-event thread to avoid blocking other connections.

    :param f: File to lock.
    :param max_retries: Number of retries for locking. Default: 11 (~1s of waiting on average).
    :return: True if lock was acquired, False otherwise.
    """
    n_tries = 0
    # While we are below the retry threshold and we don't have the lock.
    while n_tries < max_retries:
        try:
            # Try to obtain an exclusive lock on this file with a non-blocking call.
            fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            # If we made it here we have the lock.
            return True
        except OSError:
            # If we failed to get the lock, increment counter.
            n_tries += 1
            # Log some warnings if we have lock contention.
            if n_tries > 2:
                log.warning(f"Lock contention, tried to acquire {n_tries} times!")
            # Sleep with an exponential backoff.
            time.sleep(random.uniform(0, 1 << n_tries)/1000)
    # If we make it through the loop, we failed to acquire the lock.
    return False


def release_file_lock(f):
    # Release the lock
    fcntl.lockf(f, fcntl.LOCK_UN)


def try_write_to_file(b: bytes, f_p: str, max_retries: int = 11):
    """ Tries to write data to a file.

    This function performs synchronous I/O, so should only be called
    from a separate (non-event) thread.

    :param b: Byte data to write to file.
    :param f_p: Name of file to write to.
    :param max_retries: Number of times to retry lock acquisition if
    lock is not available. Default: 11 (~1s of waiting on average).
    :return: None
    """
    with open(f_p, "ab") as f:
        if not acquire_file_lock(f, max_retries=max_retries):
            raise RuntimeError(f"Unable to acquire lock for '{f_p}'.")

        # Write to the file
        n_written = f.write(b)

        # Check that we wrote everything.
        if n_written != len(b):
            log.critical(f"Buffer was length {len(b)} but wrote {n_written}!")

        # Release the lock.
        release_file_lock(f)


def try_read_from_file(f_p: str, l_stats: os.stat_result = None, max_retries: int = 11) -> tuple[bytes | None, os.stat_result]:
    """ Tries to read data from a file.

    This function performs synchronous I/O, so should only be called
    from a separate (non-event) thread.
    :param f_p: Name of file to read.
    :param l_stats: os.stat_result object from a previous call to this function. This can be used to avoid opening the file if nothing has changed.
    :param max_retries: Number of times to retry lock acquisition if lock is not available.
    :return: A tuple of (data, stats) where data is either bytes or None, and stats is an os.stat_result object.
    """
    # If l_stats is not None, we know that the file should exist
    # (so safe to call os.lstat here) since we've called it at
    # least once before.
    if l_stats is not None:
        p_stats = os.lstat(f_p)
        ret_early = False

        # If file is unmodified (same mtime), we haven't received anything.
        if p_stats.st_mtime == l_stats.st_mtime:
            ret_early = True
            if p_stats.st_size != l_stats.st_size:
                log.warning(f"st_mtime unchanged, but different content sizes (previous={p_stats.st_size}, current={l_stats.st_size}). This shouldn't happen!")
        # We would expect that if the mtime *has* changed, that there should also be content for us to read.
        # If this isn't the case, print a warning.
        elif p_stats.st_size == 0:
            ret_early = True
            log.warning("mtime has changed, but size is 0!")

        if ret_early:
            return None, p_stats

    # Open file in append mode for writing.
    with open(f_p, "ab+") as f:
        if not acquire_file_lock(f, max_retries=max_retries):
            raise RuntimeError(f"Unable to acquire lock for '{f_p}'.")

        # Get file descriptor.
        fd = f.fileno()

        # If file not empty, read it.
        if f.tell() != 0:
            # Move to the start
            f.seek(0)
            # Read the data
            all_data = f.read()
            # Truncate the file
            f.truncate(0)
        # Otherwise, we will return None.
        else:
            all_data = None
        # Get the stats of the file *after* we've read and truncated to ensure
        # that the mtime should be the same if the file has not been modified by
        # the other process.
        # TODO: is this sufficient?
        stats = os.stat(fd)

        # Release the lock
        release_file_lock(f)

    # Return data and stats.
    return all_data, stats

