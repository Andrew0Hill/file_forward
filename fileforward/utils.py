import glob
import re
import os

CON_ID_RE = re.compile("(?P<con_id>.*?)\\.(local|remote)")


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


def cleanup_connection_files(tunnel_dir: str, con_id: str):
    ib_fpath = os.path.join(tunnel_dir, f"{con_id}.remote")
    if os.path.isfile(ib_fpath):
        os.remove(ib_fpath)

    ob_fpath = os.path.join(tunnel_dir, f"{con_id}.local")
    if os.path.isfile(ob_fpath):
        os.remove(ob_fpath)