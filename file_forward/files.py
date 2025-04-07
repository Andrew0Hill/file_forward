import fcntl
import time


def acquire_file_lock(f, retry_delay: float, max_retries: int):
    have_lock = False
    n_tries = 0
    # While we are below the retry threshold and we don't have the lock.
    while (n_tries < max_retries) and (not have_lock):
        try:
            # Try to obtain an exclusive lock on this file with a non-blocking call.
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
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
    fcntl.flock(f, fcntl.LOCK_UN)


def try_write_to_file(b: bytes, f_p: str, retry_delay: int = 0.05, max_retries: int = 10):
    with open(f_p, "ab") as f:
        if not acquire_file_lock(f, retry_delay=retry_delay, max_retries=max_retries):
            raise RuntimeError(f"Unable to acquire lock for '{f_p}'.")

        # Write to the file
        n_written = f.write(b)

        # Check that we wrote everything.
        if n_written != len(b):
            print(f"Buffer was length {len(b)} but wrote {n_written}!")

        release_file_lock(f)


def try_read_from_file(f_p: str, retry_delay: int = 0.05, max_retries: int = 10):
    with open(f_p, "ab+") as f:
        if not acquire_file_lock(f, retry_delay=retry_delay, max_retries=max_retries):
            raise RuntimeError(f"Unable to acquire lock for '{f_p}'.")

        # Read from file
        f.seek(0)
        all_data = f.read()

        # Truncate the file
        f.truncate(0)

        release_file_lock(f)
    return all_data

