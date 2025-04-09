import argparse
import asyncio
import logging

from fileforward.tunnel import TunnelServer, TunnelClient
from fileforward.log import initialize_logging


def run():
    """ The main entrypoint for executing in a CLI/script environment.
    :return: None
    """

    parser = argparse.ArgumentParser(prog="fileforward")
    run_group = parser.add_mutually_exclusive_group(required=True)
    run_group.add_argument("--client", action="store_const", dest="run_type", const="client")
    run_group.add_argument("--server", action="store_const", dest="run_type", const="server")
    parser.add_argument("--port", type=int, help="The port for the client or server to connect to.", required=True)
    parser.add_argument("--tunnel_dir", type=str, help="Path to a directory to use for connections.", required=True)
    parser.add_argument("--log_level", choices=["INFO", "DEBUG"], default="DEBUG")
    parser.add_argument("--log_file_prefix", default="file_forward", help="The prefix of the log file to generate. The full log file name will be <log_file_prefix>.(client|server).log")
    parser.add_argument("--file_poll_interval", default=1.0, help="Polling interval (in seconds) for checking/reading existing connection files. Good values are small enough reasonable latency, but large enough to be good steward of I/O resources. Default: 500ms (0.5 seconds)")
    parser.add_argument("--new_conn_poll_interval", default=1.0, help="(--client only) Polling interval (in seconds) for detecting new connection files. Good values are small enough to have reasonable latency, but large enough to be good steward of I/O resources. Default: 500ms (0.5 seconds)")
    args = parser.parse_args()

    # Determine logging level
    match args.log_level:
        case "INFO":
            log_level = logging.INFO
        case "DEBUG":
            log_level = logging.DEBUG
        case invalid:
            raise RuntimeError(f"Invalid value '{invalid}' for --log_level")

    # Set up logging
    log_path = f"{args.log_file_prefix}.{args.run_type}.log"
    initialize_logging(log_path=log_path, log_level=log_level)
    log = logging.getLogger(__name__)

    # Get the appropriate runner
    match args.run_type:
        case "client":
            log.info(f"Launching TunnelClient on port {args.port}")
            run_obj = TunnelClient(remote_port=args.port, tunnel_dir=args.tunnel_dir, file_poll_interval=args.file_poll_interval, new_conn_poll_interval=args.new_conn_poll_interval)
        case "server":
            log.info(f"Launching TunnelServer on port {args.port}")
            run_obj = TunnelServer(local_port=args.port, tunnel_dir=args.tunnel_dir, file_poll_interval=args.file_poll_interval)
        case _:
            raise RuntimeError("Invalid run type.")

    # Start the async main()
    asyncio.run(run_obj.main())
