import glob
import logging
import os
import re
import math

CON_ID_RE = re.compile("(?P<con_id>.*?)\\.(local|remote)")

log = logging.getLogger(__name__)


def get_connections(tunnel_dir: str, ext: str):
    cons = glob.glob(os.path.join(tunnel_dir, ext))

    out_cons = set()
    for con_p in cons:
        con_file = os.path.split(con_p)[-1]
        con_match = CON_ID_RE.match(con_file)
        if con_match is None:
            log.warning(f"{con_p} is not a valid connection file! Please don't place files into the tunnel directory, as it will degrade performance!")
            continue
        out_cons.add(con_match.group("con_id"))
    return out_cons


def get_local_connections(tunnel_dir: str):
    return get_connections(tunnel_dir, "*.local")


def get_remote_connections(tunnel_dir: str):
    return get_connections(tunnel_dir, "*.remote")


def cleanup_connection_files(tunnel_dir: str, con_id: str):
    ib_fpath = os.path.join(tunnel_dir, f"{con_id}.remote")
    if os.path.isfile(ib_fpath):
        log.debug(f"{con_id} Remote connection file exists, removing.")
        os.remove(ib_fpath)

    ob_fpath = os.path.join(tunnel_dir, f"{con_id}.local")
    if os.path.isfile(ob_fpath):
        log.debug(f"{con_id} Local connection file exists, removing.")
        os.remove(ob_fpath)


def memory_str(n_bytes: int):
    if n_bytes == 0:
        return "0B"

    log_bytes = math.log10(math.fabs(n_bytes))

    bytes_l = ["B", "KB", "MB", "GB", "TB"]
    bytes_s = [3, 6, 9, 12, math.inf]

    # Find the largest unit >1 that represents the size
    byte_sub = 0
    for byte_l, byte_s in zip(bytes_l, bytes_s):
        byte_lbl = byte_l
        if log_bytes < byte_s:
            break
        byte_sub = byte_s

    return f"{'-' if n_bytes < 0 else ''}{math.pow(10, log_bytes - byte_sub):0.1f}{byte_lbl}"
