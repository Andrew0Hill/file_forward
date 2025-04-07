import asyncio
import glob
import logging
import os
from asyncio import CancelledError
from functools import partial

from . import tasks
from . import utils

log = logging.getLogger(__name__)


class TunnelClient:

    def __init__(self, remote_port: int, tunnel_dir: str, new_conn_poll_interval: float | int, file_poll_interval: float | int):
        # Variables
        self.remote_port = remote_port
        self.tunnel_dir = tunnel_dir
        self.new_conn_poll_int = new_conn_poll_interval
        self.file_poll_int = file_poll_interval
        # Active Connections.
        self.active_cons = {}

    @staticmethod
    def con_id_to_path_name(con_paths):
        return {os.path.split(p)[-1]: p for p in con_paths}

    def remove_connection_by_id(self, t):
        task_name = t.get_name()
        log.info(f"{task_name} Attempting to remove completed connection...")
        if task_name in self.active_cons:
            del self.active_cons[task_name]
            log.info(f"{task_name} Connection removed from set.")
        else:
            log.info(f"{task_name} Connection id not found in set!!!!")

    async def poll_for_connections(self):
        log.debug("Checking for new forwarded connections...")
        local_cons = utils.get_local_connections(self.tunnel_dir)

        # Any cons that are in the directory but *not* in our running set are new.
        new_cons = local_cons - self.active_cons.keys()
        # Any cons that are in our running set but not the directory are old/closed.
        removed_cons = self.active_cons.keys() - local_cons

        for new_con in new_cons:
            log.info(f"Creating new client connection for {new_con}")
            try:
                reader, writer = await asyncio.open_connection("localhost", self.remote_port)
                file_forward_task = asyncio.create_task(tasks.file_forwarding_task(reader, writer, tunnel_dir=self.tunnel_dir, caller="client", con_id=new_con, file_poll_int=self.file_poll_int), name=new_con)
                file_forward_task.add_done_callback(self.remove_connection_by_id)
                self.active_cons[new_con] = file_forward_task
            except OSError:
                log.error(f"{new_con} Unable to connect to remote host, is anything running on port {self.remote_port}?")

        for rm_con in removed_cons:
            log.info(f"{rm_con} Attempting to shut down old connection...")
            if rm_con in self.active_cons:
                log.info(f"{rm_con} Found old connection in active connection set...")
                self.active_cons[rm_con].cancel()
                del self.active_cons[rm_con]
                log.info(f"{rm_con} Connection task cancelled and deleted.")
            else:
                log.info(f"{rm_con} Didn't find connection in active connection set, maybe it was removed earlier?")

    async def main(self):
        try:
            # Poll the tunnel directory for new connections every 'self.connection_poll_int' seconds.
            while True:
                await self.poll_for_connections()
                await asyncio.sleep(self.new_conn_poll_int)
        except CancelledError:
            log.warning("Client loop cancelled, will attempt to cancel all connections.")
            active_con_list = list(self.active_cons.values())
            if len(active_con_list) != 0:
                for active_t in self.active_cons.values():
                    active_t.cancel()
                log.info("Waiting for all connection tasks to complete...")
                await asyncio.wait(self.active_cons.values())
                log.info("All connections finished, client exiting gracefully.")
            else:
                log.info("No active connections, client exiting gracefully.")


class TunnelServer:
    def __init__(self, local_port: int, tunnel_dir: str, file_poll_interval: int | float):
        # Variables
        self.local_port = local_port
        self.tunnel_dir = tunnel_dir
        self.file_poll_int = file_poll_interval
        # Old tunnel cleanup.
        self.clean_tunnel_dir()

    def clean_tunnel_dir(self):
        if os.path.isdir(self.tunnel_dir):
            ob_cons = glob.glob(os.path.join(self.tunnel_dir, "*.local"))
            ib_cons = glob.glob(os.path.join(self.tunnel_dir, "*.remote"))
            old_cons = ib_cons + ob_cons
            if len(old_cons) > 0:
                log.info(f"Cleaning up {len(old_cons)} old connection files...")
                for old_con_p in old_cons:
                    os.remove(old_con_p)
                log.info("Cleaned up old connection files.")
        else:
            log.info(f"Tunnel directory: '{self.tunnel_dir}' does not exist, creating...")
            os.makedirs(self.tunnel_dir, exist_ok=True)

    async def main(self):
        # We use a partial here since asyncio.start_server expects the callback to only have two arguments.
        server_task = partial(tasks.file_forwarding_task, tunnel_dir=self.tunnel_dir, caller="server", file_poll_int=self.file_poll_int)
        # Run the server.
        server = await asyncio.start_server(server_task, "localhost", self.local_port)

        async with server:
            try:
                await server.serve_forever()
            except CancelledError:
                log.warning("Server task cancelled.")
                server.close()
                await server.wait_closed()
                log.info("Server closed gracefully.")

