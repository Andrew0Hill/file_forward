import logging
import fcntl
import time
import glob
import re
import os


CON_ID_RE = re.compile("(?P<con_id>.*?)\\.(local|remote)")


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


async def close_writer(writer):
    if not writer.is_closing():
        writer.close()
        await writer.wait_closed()


# async def try_read(reader, n_bytes: int = 4096):
#     try:
#         data = await reader.read(n_bytes)
#     except Exception as e:
#         print(f"Read Exception: {e}")
#         return b""
#     return data


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


def get_connections(tunnel_dir: str, ext: str):
    cons = glob.glob(os.path.join(tunnel_dir, ext))

    out_cons = set()
    for con_p in cons:
        con_file = os.path.split(con_p)[-1]
        con_match = CON_ID_RE.match(con_file)
        if con_match is None:
            print(f"Unable to generate connection id for file {con_p}, please don't place files into the tunnel directory!")
            continue
        out_cons.add(con_match.group("con_id"))
    return out_cons


def get_local_connections(tunnel_dir: str):
    return get_connections(tunnel_dir, "*.local")


def get_remote_connections(tunnel_dir: str):
    return get_connections(tunnel_dir, "*.remote")

