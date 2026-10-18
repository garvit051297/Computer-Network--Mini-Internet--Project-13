
import json
import logging
import os
import socket
import sys
import uuid
from datetime import datetime, timezone

from packet import encode_packet, decode_packet, MAX_PACKET_SIZE


CONFIG_FILE = "config.json"
RECEIVE_BUFFER_SIZE = MAX_PACKET_SIZE + 1


def load_config():
    """Load and validate the network configuration."""
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as file:
            config = json.load(file)
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Could not load {CONFIG_FILE}: {error}")

    for key in ("nodes", "links", "forwarding_tables"):
        if key not in config:
            raise ValueError(f"Missing '{key}' in {CONFIG_FILE}")

    for name, node in config["nodes"].items():
        if not isinstance(node.get("udp_port"), int):
            raise ValueError(f"Invalid UDP port for {name}")
        if not 1 <= node["udp_port"] <= 65535:
            raise ValueError(f"UDP port out of range for {name}")
        if not node.get("virtual_ip"):
            raise ValueError(f"Missing virtual IP for {name}")

    return config


def setup_logger(router_name, run_id):
    """Write router events to both the terminal and a log file."""
    os.makedirs("logs", exist_ok=True)

    logger = logging.getLogger(router_name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(message)s")

    file_handler = logging.FileHandler(
        os.path.join("logs", f"{router_name}.log"),
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(json.dumps({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "device": router_name,
        "event": "START",
        "message": f"{router_name} starting"
    }))

    return logger


def log_event(logger, run_id, router_name, event, **details):
    """Write a structured record for a router event."""
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "device": router_name,
        "event": event
    }
    record.update(details)
    logger.info(json.dumps(record))


def is_neighbor(config, router_name, neighbor_name):
    """Treat each configured physical link as bidirectional."""
    for link in config["links"]:
        endpoints = {link.get("from"), link.get("to")}
        if endpoints == {router_name, neighbor_name}:
            return True
    return False


def run_router(router_name):
    config = load_config()
    nodes = config["nodes"]

    if router_name not in nodes:
        raise ValueError(f"Unknown node: {router_name}")

    if nodes[router_name].get("type") != "router":
        raise ValueError(f"{router_name} is not configured as a router")

    if router_name not in config["forwarding_tables"]:
        raise ValueError(f"No forwarding table configured for {router_name}")

    router = nodes[router_name]
    router_ip = router["virtual_ip"]
    router_port = router["udp_port"]
    forwarding_table = config["forwarding_tables"][router_name]

    # Map virtual IP addresses to node names.
    ip_to_name = {
        node["virtual_ip"]: name
        for name, node in nodes.items()
    }

    run_id = str(uuid.uuid4())
    logger = setup_logger(router_name, run_id)

    log_event(
        logger, run_id, router_name, "LISTEN",
        virtual_ip=router_ip,
        udp_endpoint=f"127.0.0.1:{router_port}"
    )

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", router_port))

        while True:
            data, source_endpoint = sock.recvfrom(RECEIVE_BUFFER_SIZE)

            # Decode and reject malformed or oversized packets.
            packet = decode_packet(data)
            if packet is None:
                log_event(
                    logger, run_id, router_name, "DROP",
                    reason="malformed_or_oversized_packet",
                    udp_source=f"{source_endpoint[0]}:{source_endpoint[1]}"
                )
                continue

            src_ip = packet["src_ip"]
            dst_ip = packet["dst_ip"]
            msg_id = packet["msg_id"]
            msg_type = packet["msg_type"]
            hop_limit = packet["hop_limit"]

            common = {
                "msg_id": msg_id,
                "msg_type": msg_type,
                "src_ip": src_ip,
                "dst_ip": dst_ip,
                "hop_limit": hop_limit,
                "udp_source": (
                    f"{source_endpoint[0]}:{source_endpoint[1]}"
                )
            }

            log_event(
                logger, run_id, router_name, "RECEIVE",
                **common
            )

            # A router does not act as a host application.
            if dst_ip == router_ip:
                log_event(
                    logger, run_id, router_name, "LOCAL_DESTINATION",
                    **common
                )
                continue

            # Find the next-hop neighbor using the destination virtual IP.
            next_hop = forwarding_table.get(dst_ip)

            if next_hop is None:
                log_event(
                    logger, run_id, router_name, "DROP",
                    reason="unknown_destination",
                    **common
                )
                continue

            if next_hop not in nodes:
                log_event(
                    logger, run_id, router_name, "DROP",
                    reason="invalid_next_hop",
                    next_hop=next_hop,
                    **common
                )
                continue

            if not is_neighbor(config, router_name, next_hop):
                log_event(
                    logger, run_id, router_name, "DROP",
                    reason="next_hop_not_adjacent",
                    next_hop=next_hop,
                    **common
                )
                continue

            # Do not forward a packet whose hop limit is exhausted.
            if hop_limit <= 1:
                log_event(
                    logger, run_id, router_name, "DROP",
                    reason="hop_limit_exhausted",
                    next_hop=next_hop,
                    **common
                )
                continue

            # Resolve the neighbor's configured UDP endpoint.
            neighbor = nodes[next_hop]
            next_endpoint = ("127.0.0.1", neighbor["udp_port"])

            # Re-encode the packet with the hop limit decremented.
            try:
                outgoing = encode_packet(
                    src_ip=src_ip,
                    dst_ip=dst_ip,
                    hop_limit=hop_limit - 1,
                    msg_type=msg_type,
                    msg_id=msg_id,
                    payload_str=packet["payload"]
                )
            except (ValueError, TypeError) as error:
                log_event(
                    logger, run_id, router_name, "DROP",
                    reason="packet_encoding_error",
                    error=str(error),
                    **common
                )
                continue

            sock.sendto(outgoing, next_endpoint)

            log_event(
                logger, run_id, router_name, "FORWARD",
                msg_id=msg_id,
                msg_type=msg_type,
                src_ip=src_ip,
                dst_ip=dst_ip,
                incoming_hop_limit=hop_limit,
                outgoing_hop_limit=hop_limit - 1,
                next_hop=next_hop,
                udp_destination=(
                    f"{next_endpoint[0]}:{next_endpoint[1]}"
                )
            )


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python router.py ROUTER_NAME")
        print("Example: python router.py R_A")
        sys.exit(1)

    try:
        run_router(sys.argv[1])
    except KeyboardInterrupt:
        print("\nRouter stopped.")
    except (RuntimeError, ValueError, OSError) as error:
        print(f"Router startup/runtime error: {error}")
        sys.exit(1)
