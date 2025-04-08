import fcntl
import logging
import os
import time

log = logging.getLogger(__name__)


def acquire_file_lock(f, retry_delay: float, max_retries: int):
    have_lock = False
    n_tries = 0
    # While we are below the retry threshold and we don't have the lock.
    while (n_tries < max_retries) and (not have_lock):
        try:
            # Try to obtain an exclusive lock on this file with a non-blocking call.
            fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            # If we made it here we have the lock.
            have_lock = True
        except OSError:
            # If we failed to get the lock, increment counter.
            n_tries += 1
            # Sleep (yield to other tasks) and try again later.
            time.sleep(retry_delay)
    return have_lock


def release_file_lock(f):
    # Release the lock
    fcntl.lockf(f, fcntl.LOCK_UN)


def try_write_to_file(b: bytes, f_p: str, retry_delay: int = 0.05, max_retries: int = 10):
    with open(f_p, "ab") as f:
        if not acquire_file_lock(f, retry_delay=retry_delay, max_retries=max_retries):
            raise RuntimeError(f"Unable to acquire lock for '{f_p}'.")

        # Write to the file
        n_written = f.write(b)

        # Check that we wrote everything.
        if n_written != len(b):
            log.warning(f"Buffer was length {len(b)} but wrote {n_written}!")

        release_file_lock(f)


def try_read_from_file(f_p: str, l_stats: os.stat_result = None, retry_delay: int = 0.05, max_retries: int = 10):
    # We use a few checks to prevent opening/reading the file as much as possible.
    #   1. If we have read this file before, check if the m_time is different.
    #   2. If the file has st_size==0, skip reading.
    if l_stats is not None:
        p_stats = os.lstat(f_p)
        ret_early = False

        # If file is unmodified (same mtime), we haven't received anything.
        if p_stats.st_mtime == l_stats.st_mtime:
            ret_early = True
            #log.info("Skip read, st_mtime unchanged.")
            if p_stats.st_size != 0:
                log.warning(f"st_mtime unchanged, but has content (st_size={p_stats.st_size}). This shouldn't happen!")
        # We would expect that if the mtime *has* changed, that there should also be content for us to read.
        # If this isn't the case, print a warning.
        elif p_stats.st_size == 0:
            ret_early = True
            log.warning("mtime has changed, but size is 0!")

        if ret_early:
            return b"", p_stats

    with open(f_p, "ab+") as f:
        if not acquire_file_lock(f, retry_delay=retry_delay, max_retries=max_retries):
            raise RuntimeError(f"Unable to acquire lock for '{f_p}'.")
        # File descriptor
        fd = f.fileno()

        # Read from file
        f.seek(0)
        all_data = f.read()

        # Truncate the file
        f.truncate(0)

        # Get the stats of the file *after* we've read and truncated to ensure
        # that the mtime should be the same if the file has not been modified by
        # the other process.
        # TODO: is this sufficient?
        stats = os.stat(fd)

        # Release the lock and return to
        release_file_lock(f)
    return all_data, stats

