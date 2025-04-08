import asyncio
import tasks
import os

import tunnel


class AsyncTunnelClient:

    def __init__(self, forward_address: tuple, tunnel_dir: str):
        # Variables
        self.forward_address = forward_address
        self.tunnel_dir = tunnel_dir
        self.active_cons = {}

    @staticmethod
    def con_id_to_path_name(con_paths):
        return {os.path.split(p)[-1]: p for p in con_paths}

    def remove_connection_by_id(self, t):
        task_name = t.get_name()
        print(f"{task_name} Attempting to remove completed connection...")
        if task_name in self.active_cons:
            del self.active_cons[task_name]
            print(f"{task_name} Connection removed from set.")
        else:
            print(f"{task_name} Connection id not found in set!!!!")

    async def poll_for_connections(self):
        local_cons = tunnel.get_local_connections(self.tunnel_dir)
        # Any cons that are in the directory but *not* in our running set are new.
        new_cons = local_cons - self.active_cons.keys()
        # Any cons that are in our running set but not the directory are old/closed.
        removed_cons = self.active_cons.keys() - local_cons

        for new_con in new_cons:
            print(f"Creating new connection for {new_con}")
            try:
                reader, writer = await asyncio.open_connection(self.forward_address[0], self.forward_address[1])
                file_forward_task = asyncio.create_task(tasks.file_forwarding_task(reader, writer, tunnel_dir=self.tunnel_dir, caller="client", con_id=new_con), name=new_con)
                file_forward_task.add_done_callback(self.remove_connection_by_id)
                self.active_cons[new_con] = file_forward_task
            except Exception as e:
                print("Unable to connect to remote host, remote server may not be running yet?")

        for rm_con in removed_cons:
            print(f"{rm_con} Attempting to shut down old connection...")
            if rm_con in self.active_cons:
                print(f"{rm_con} Found old connection in active connection set...")
                self.active_cons[rm_con].cancel()
                del self.active_cons[rm_con]
                print(f"{rm_con} Connection task cancelled and deleted.")
            else:
                print(f"{rm_con} Didn't find connection in active connection set, maybe it was removed earlier?")

    async def main(self):
        # Poll the tunnel directory for new connections.
        while True:
            await self.poll_for_connections()
            await asyncio.sleep(5)


if __name__ == "__main__":
    atc = AsyncTunnelClient(forward_address=("localhost", 8888), tunnel_dir="my_tunnel")
    asyncio.run(atc.main())
