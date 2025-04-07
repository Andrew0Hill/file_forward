import asyncio
import uuid
import glob
import fcntl
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

    async def check_for_connections(self):
        local_cons = tunnel.get_local_connections(self.tunnel_dir)

        # Any cons that are in the directory but *not* in our running set are new.
        new_cons = local_cons - self.active_cons.keys()
        # Any cons that are in our running set but not the directory are old/closed.
        removed_cons = self.active_cons.keys() - local_cons

        for new_con in new_cons:
            print(f"Creating new connection for {new_con}")
            con_task = asyncio.create_task(self.create_and_monitor_connection(con_id=new_con), name=new_con)
            con_task.add_done_callback(self.remove_connection_by_id)
            self.active_cons[new_con] = con_task

        for rm_con in removed_cons:
            print(f"{rm_con} Attempting to shut down old connection...")
            if rm_con in self.active_cons:
                print(f"{rm_con} Found old connection in active connection set...")
                self.active_cons[rm_con].cancel()
                del self.active_cons[rm_con]
                print(f"{rm_con} Connection task cancelled and deleted.")
            else:
                print(f"{rm_con} Didn't find connection in active connection set, maybe it was removed earlier?")

    async def create_and_monitor_connection(self, con_id: str):
        try:
            # A connection to the forwarding address
            fw_reader, fw_writer = await asyncio.open_connection(self.forward_address[0], self.forward_address[1])
            tasks = []
            try:
                # Pull task reads from the file and writes to the forwarded connection.
                pull_task = asyncio.create_task(self.pull_from_file(con_id, fw_writer))
                tasks.append(pull_task)
                # Push task reads from the forwarded connection and writes to the file.
                push_task = asyncio.create_task(self.push_to_file(con_id, fw_reader))
                tasks.append(push_task)
                # If either end breakss, we will complete this future.
                completed, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                # If the non-completed task is still pending, cancel it.
                for pend_t in pending:
                    pend_t.cancel()
            except Exception as e:
                print(f"{con_id} Error: {e}")
            finally:
                # Try to cancel everything we can.
                for task in tasks:
                    task.cancel()
                # Close the forwarding writer we opened.
                await tunnel.close_writer(fw_writer)
        except Exception as e:
            print(f"{con_id} Connection Error: {e}")

    async def push_to_file(self, con_id: str, reader, n_bytes: int = 4096):
        # TODO: refactor these loops to instead be two separate tasks (i.e. 4 tasks per connection).
        # 2 queues (local, remote) for data transfers
        # Task 1 - Read from local connection, write to local queue
        # Task 2 - Read from local queue, write to file.
        # Task 3 - Read from remote file, write to remote queue.
        # Task 4 - Read from remote queue, write to remote connection
        loop = asyncio.get_running_loop()
        # Name of inbound file.
        ib_fpath = os.path.join(self.tunnel_dir, f"{con_id}.remote")
        # Read data from socket
        data = await reader.read(n_bytes)
        while len(data) != 0:
            # Write data to disk
            await loop.run_in_executor(None, tunnel.try_write_to_file, data, ib_fpath)
            # Wait for response.
            data = await reader.read(n_bytes)
        print(f"{con_id} socket reader closed.")

    async def pull_from_file(self, con_id: str, writer, n_bytes: int = 4096):
        loop = asyncio.get_running_loop()
        # Name of outbound file
        ob_fpath = os.path.join(self.tunnel_dir, f"{con_id}.local")
        # Read data from file
        data = await loop.run_in_executor(None, tunnel.try_read_from_file, ob_fpath)
        while True:
            # Push data to socket
            writer.write(data)
            await writer.drain()
            data = await loop.run_in_executor(None, tunnel.try_read_from_file, ob_fpath)
        print(f"{con_id} socket writer closed.")

    async def main(self):
        # Poll the tunnel directory for new connections.
        while True:
            await self.check_for_connections()
            await asyncio.sleep(1)


if __name__ == "__main__":
    atc = AsyncTunnelClient(forward_address=("localhost", 8888), tunnel_dir="my_tunnel")
    asyncio.run(atc.main())
