import asyncio
import glob
import logging
import os
from asyncio import CancelledError
from functools import partial

from fileforward import tasks
from fileforward import utils

log = logging.getLogger(__name__)


class TunnelClient:
    """ TunnelClient is responsible for reading
    """
    def __init__(self, remote_port: int, tunnel_dir: str, new_conn_poll_interval: float | int, file_poll_interval: float | int):
        # Variables
        self.remote_port = remote_port
        self.tunnel_dir = tunnel_dir
        self.new_conn_poll_int = new_conn_poll_interval
        self.file_poll_int = file_poll_interval
        # Active Connections.
        self.active_cons = {}

    @staticmethod
    def _con_id_to_path_name(con_paths):
        return {os.path.split(p)[-1]: p for p in con_paths}

    def _remove_connection_task(self, t):
        task_name = t.get_name()
        log.debug(f"{task_name} Attempting to remove completed connection...")
        if task_name in self.active_cons:
            del self.active_cons[task_name]
            log.debug(f"{task_name} Connection removed from set.")
        else:
            log.debug(f"{task_name} Connection not found in set, must have already been removed.")

    async def _remove_connection_by_id(self, con_id: str):
        log.debug(f"{con_id} Attempting to cancel...")

        con: asyncio.Task = self.active_cons.get(con_id)
        if con is not None:
            log.debug(f"{con_id} Found in active connections, trying to cancel.")
            if not con.done():
                con.cancel()
                try:
                    await con
                except CancelledError:
                    log.debug(f"{con_id} Finished.")
                except:
                    log.debug(f"{con_id} Finished with error.")
        # There is an async gap so we check again before deleting
        if con_id in self.active_cons:
            del self.active_cons[con_id]

    async def _poll(self):
        # Get all local (incoming) connections.
        local_cons = utils.get_local_connections(self.tunnel_dir)

        # Any cons that are in the directory but *not* in our running set are new.
        new_cons = local_cons - self.active_cons.keys()

        # Any cons that are in our running set but not the directory are old/closed.
        removed_cons = self.active_cons.keys() - local_cons

        # Iterate and create a new Task for each incoming connection.
        for new_con in new_cons:
            log.info(f"Creating new client connection for {new_con}")
            try:
                reader, writer = await asyncio.open_connection("localhost", self.remote_port)
                file_forward_task = asyncio.create_task(tasks.file_forwarding_task(reader, writer, tunnel_dir=self.tunnel_dir, caller="client", con_id=new_con, file_poll_int=self.file_poll_int), name=new_con)
                file_forward_task.add_done_callback(self._remove_connection_task)
                self.active_cons[new_con] = file_forward_task
            except OSError:
                log.error(f"{new_con} Unable to connect to remote host, is anything running on port {self.remote_port}?")

        # Remove all old connections.
        await asyncio.gather(*(self._remove_connection_by_id(rm_id) for rm_id in removed_cons), return_exceptions=True)

    async def main(self):
        try:
            # Poll the tunnel directory for new connections every 'self.new_conn_poll_int' seconds.
            while True:
                await self._poll()
                await asyncio.sleep(self.new_conn_poll_int)
        # If we get a cancellation, try to clean up the connections.
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

