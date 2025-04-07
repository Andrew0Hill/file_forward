import asyncio
import uuid
import glob
import fcntl
import os
import tunnel


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

    def create_connection_file(self, con_id):
        fpath = os.path.join(self.tunnel_dir, f"{con_id}.local")
        assert not os.path.exists(fpath)
        open(fpath, "wb").close()
        return fpath

    async def push_to_file(self, con_id: str, reader, n_bytes: int = 4096):
        loop = asyncio.get_running_loop()
        # Name of outbound file.
        ob_fpath = os.path.join(self.tunnel_dir, f"{con_id}.local")
        # Read data from socket
        data = await reader.read(n_bytes)
        while len(data) != 0:
            # Write data to disk
            await loop.run_in_executor(None, tunnel.try_write_to_file, data, ob_fpath)
            # Wait for response.
            data = await reader.read(n_bytes)

    async def pull_from_file(self, con_id: str, writer, n_bytes: int = 4096):
        loop = asyncio.get_running_loop()
        # Name of inbound file
        ib_fpath = os.path.join(self.tunnel_dir, f"{con_id}.remote")
        # Read data from file
        data = await loop.run_in_executor(None, tunnel.try_read_from_file, ib_fpath)
        while True:
            # Push data to socket
            writer.write(data)
            await writer.drain()
            data = await loop.run_in_executor(None, tunnel.try_read_from_file, ib_fpath)

    async def handle_connection(self, lc_reader, lc_writer):
        con_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        # Open the forwarding connections
        print(f"New connection: {con_id}")
        tasks = []
        try:
            push_task = asyncio.create_task(self.push_to_file(con_id, lc_reader))
            tasks.append(push_task)
            pull_task = asyncio.create_task(self.pull_from_file(con_id, lc_writer))
            tasks.append(pull_task)
            print(f"{con_id} Waiting for tasks to complete...")
            completed, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            print(f"{con_id} first task completed, cancelling others...")
            for task in pending:
                task.cancel()
            print(f"{con_id} cleaning up files...")
            await loop.run_in_executor(None, self.cleanup_connection, con_id)
        except Exception as e:
            print(f"Connection: {con_id} Error: {e}")
            for task in tasks:
                task.cancel()
        finally:
            await tunnel.close_writer(lc_writer)
            await loop.run_in_executor(None, self.cleanup_connection, con_id)
            print(f"{con_id} connection finished cleanly.")

    def cleanup_connection(self, con_id: str):
        ib_fpath = os.path.join(self.tunnel_dir, f"{con_id}.remote")
        if os.path.isfile(ib_fpath):
            os.remove(ib_fpath)

        ob_fpath = os.path.join(self.tunnel_dir, f"{con_id}.local")
        if os.path.isfile(ob_fpath):
            os.remove(ob_fpath)

    async def main(self):
        server = await asyncio.start_server(self.handle_connection, self.local_address[0], self.local_address[1])

        addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
        print(f'Serving on {addrs}')

        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    afs = AsyncTunnelServer(local_address=("localhost", 8893), tunnel_dir="my_tunnel")
    asyncio.run(afs.main(), debug=True)
