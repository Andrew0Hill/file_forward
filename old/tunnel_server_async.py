import asyncio
import glob
import os
import tasks
from functools import partial


class AsyncTunnelServer:
    def __init__(self, local_address: tuple, tunnel_dir: str):
        # Variables
        self.local_address = local_address
        self.tunnel_dir = tunnel_dir
        # Old tunnel cleanup.
        self.clean_tunnel_dir()

    def clean_tunnel_dir(self):
        if os.path.isdir(self.tunnel_dir):
            ob_cons = glob.glob(os.path.join(self.tunnel_dir, "*.local"))
            ib_cons = glob.glob(os.path.join(self.tunnel_dir, "*.remote"))
            old_cons = ib_cons + ob_cons
            if len(old_cons) > 0:
                print(f"Cleaning up {len(old_cons)} old connection files...")
                for old_con_p in old_cons:
                    os.remove(old_con_p)
                print("Done.")
        else:
            os.makedirs(self.tunnel_dir, exist_ok=True)

    async def main(self):
        # We use a partial here since asyncio.start_server expects the callback to only have two arguments.
        server_task = partial(tasks.file_forwarding_task, tunnel_dir=self.tunnel_dir, caller="server")
        # Run the server.
        server = await asyncio.start_server(server_task, self.local_address[0], self.local_address[1])

        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    afs = AsyncTunnelServer(local_address=("localhost", 8893), tunnel_dir="my_tunnel")
    asyncio.run(afs.main(), debug=True)
