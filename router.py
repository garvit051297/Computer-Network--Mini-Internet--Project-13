
import json
import logging
import os
import random
import socket
import sys
import threading
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
        port = node.get("udp_port")
        if isinstance(port, bool) or not isinstance(port, int):
            raise ValueError(f"Invalid UDP port for {name}")
        if not 1 <= port <= 65535:
            raise ValueError(f"UDP port out of range for {name}")
        if not node.get("virtual_ip"):
            raise ValueError(f"Missing virtual IP for {name}")

    for index, link in enumerate(config["links"]):
        if not isinstance(link, dict):
            raise ValueError(f"Link {index} must be an object")

        delay_ms = link.get("delay_ms", 0)
        loss_prob = link.get("loss_prob", 0.0)

        if (
            isinstance(delay_ms, bool)
            or not isinstance(delay_ms, (int, float))
            or delay_ms < 0
        ):
            raise ValueError(
                f"Invalid delay_ms on link {index}; it must be non-negative"
            )

        if (
            isinstance(loss_prob, bool)
            or not isinstance(loss_prob, (int, float))
            or not 0.0 <= loss_prob <= 1.0
        ):
            raise ValueError(
                f"Invalid loss_prob on link {index}; use a value from 0 to 1"
            )

    return config


def setup_logger(router_name, run_id, seed=None):
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
        "seed": seed,
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


def get_link_settings(config, node_a, node_b):
    """Return delay and loss settings for a link in either direction."""
    for link in config["links"]:
        endpoints = {link.get("from"), link.get("to")}
        if endpoints == {node_a, node_b}:
            return (
                link.get("delay_ms", 0),
                link.get("loss_prob", 0.0)
            )

    raise ValueError(f"No configured link between {node_a} and {node_b}")


def run_router(
    router_name,
    seed=None,
    drop_first_data=False,
    drop_final_ack=False,
    corrupt_first_data=False
):
    """Run a router with optional deterministic packet test hooks."""
    if seed is not None:
        random.seed(seed)

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

    run_id = str(uuid.uuid4())
    logger = setup_logger(router_name, run_id, seed=seed)

    # The final DATA packet identifies the final sequence number.
    final_sequences = {}
    dropped_final_ack = False

    # Track whether the intentional corruption has already occurred.
    corrupted_first_data = False

    log_event(
        logger, run_id, router_name, "LISTEN",
        virtual_ip=router_ip,
        udp_endpoint=f"127.0.0.1:{router_port}",
        seed=seed,
        drop_first_data=drop_first_data,
        drop_final_ack=drop_final_ack,
        corrupt_first_data=corrupt_first_data
    )

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", router_port))

        while True:
            data, source_endpoint = sock.recvfrom(RECEIVE_BUFFER_SIZE)

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
            payload = packet["payload"]

            common = {
                "msg_id": msg_id,
                "msg_type": msg_type,
                "src_ip": src_ip,
                "dst_ip": dst_ip,
                "hop_limit": hop_limit,
                "udp_source": f"{source_endpoint[0]}:{source_endpoint[1]}"
            }

            log_event(logger, run_id, router_name, "RECEIVE", **common)

            if dst_ip == router_ip:
                log_event(
                    logger, run_id, router_name, "LOCAL_DESTINATION",
                    **common
                )
                continue

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

            if hop_limit <= 1:
                log_event(
                    logger, run_id, router_name, "DROP",
                    reason="hop_limit_exhausted",
                    next_hop=next_hop,
                    **common
                )
                continue

            try:
                delay_ms, loss_prob = get_link_settings(
                    config, router_name, next_hop
                )
            except ValueError as error:
                log_event(
                    logger, run_id, router_name, "DROP",
                    reason="link_settings_error",
                    error=str(error),
                    next_hop=next_hop,
                    **common
                )
                continue

            # Existing deterministic DATA-loss test.
            if (
                drop_first_data
                and router_name == "R_B"
                and src_ip == "10.0.0.1"
                and dst_ip == "10.0.0.6"
                and msg_type == "DATA"
            ):
                log_event(
                    logger, run_id, router_name, "DROP",
                    reason="intentional_test_drop",
                    next_hop=next_hop,
                    test="drop_first_h1_to_h2_data",
                    **common
                )
                drop_first_data = False
                continue

            # Learn the final sequence number from the final DATA payload.
            if (
                router_name == "R_B"
                and src_ip == "10.0.0.1"
                and dst_ip == "10.0.0.6"
                and msg_type == "DATA"
            ):
                try:
                    message = json.loads(payload)
                    if (
                        isinstance(message, dict)
                        and message.get("is_last") is True
                        and isinstance(message.get("seq"), int)
                        and not isinstance(message.get("seq"), bool)
                    ):
                        transfer_id = message.get("transfer_id")
                        if isinstance(transfer_id, str):
                            final_sequences[transfer_id] = message["seq"]
                except (json.JSONDecodeError, TypeError):
                    pass

            # Drop the first matching ACK for the learned final DATA sequence.
            if (
                drop_final_ack
                and not dropped_final_ack
                and router_name == "R_B"
                and src_ip == "10.0.0.6"
                and dst_ip == "10.0.0.1"
                and msg_type == "ACK"
            ):
                try:
                    ack = json.loads(payload)
                    transfer_id = ack.get("transfer_id")
                    ack_seq = ack.get("seq")

                    if (
                        isinstance(ack, dict)
                        and isinstance(transfer_id, str)
                        and isinstance(ack_seq, int)
                        and not isinstance(ack_seq, bool)
                        and final_sequences.get(transfer_id) == ack_seq
                    ):
                        log_event(
                            logger, run_id, router_name, "DROP",
                            reason="intentional_final_ack_drop",
                            test="drop_first_final_ack",
                            transfer_id=transfer_id,
                            seq=ack_seq,
                            next_hop=next_hop,
                            **common
                        )
                        dropped_final_ack = True
                        continue
                except (json.JSONDecodeError, AttributeError, TypeError):
                    pass

            if random.random() < loss_prob:
                log_event(
                    logger, run_id, router_name, "DROP",
                    reason="simulated_link_loss",
                    next_hop=next_hop,
                    delay_ms=delay_ms,
                    loss_prob=loss_prob,
                    **common
                )
                continue

            # Bonus test: intentionally corrupt the first matching DATA packet.
            # The CRC-32 field is deliberately left unchanged.
            if (
                corrupt_first_data
                and not corrupted_first_data
                and router_name == "R_B"
                and src_ip == "10.0.0.1"
                and dst_ip == "10.0.0.6"
                and msg_type == "DATA"
            ):
                try:
                    message = json.loads(payload)

                    if (
                        isinstance(message, dict)
                        and isinstance(message.get("data"), str)
                        and isinstance(message.get("crc32"), str)
                    ):
                        original_data = message["data"]

                        if original_data:
                            message["data"] = "X" + original_data[1:]
                        else:
                            message["data"] = "X"

                        # Preserve the original checksum to simulate corruption.
                        original_checksum = message["crc32"]

                        payload = json.dumps(
                            message,
                            ensure_ascii=False,
                            separators=(",", ":")
                        )

                        log_event(
                            logger, run_id, router_name, "CORRUPT",
                            reason="intentional_crc32_test_corruption",
                            test="corrupt_first_h1_to_h2_data",
                            transfer_id=message.get("transfer_id"),
                            seq=message.get("seq"),
                            original_crc32=original_checksum,
                            checksum_modified=False,
                            next_hop=next_hop,
                            **common
                        )

                        corrupted_first_data = True

                except (json.JSONDecodeError, TypeError, ValueError) as error:
                    log_event(
                        logger, run_id, router_name, "CORRUPTION_TEST_ERROR",
                        reason="could_not_corrupt_data_payload",
                        error=str(error),
                        **common
                    )

            neighbor = nodes[next_hop]
            next_endpoint = ("127.0.0.1", neighbor["udp_port"])

            try:
                outgoing = encode_packet(
                    src_ip=src_ip,
                    dst_ip=dst_ip,
                    hop_limit=hop_limit - 1,
                    msg_type=msg_type,
                    msg_id=msg_id,
                    payload_str=payload
                )
            except (ValueError, TypeError) as error:
                log_event(
                    logger, run_id, router_name, "DROP",
                    reason="packet_encoding_error",
                    error=str(error),
                    **common
                )
                continue

            def forward_packet(
                outgoing=outgoing,
                next_endpoint=next_endpoint,
                next_hop=next_hop,
                delay_ms=delay_ms,
                loss_prob=loss_prob,
                src_ip=src_ip,
                dst_ip=dst_ip,
                msg_id=msg_id,
                msg_type=msg_type,
                hop_limit=hop_limit
            ):
                try:
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
                        ),
                        delay_ms=delay_ms,
                        loss_prob=loss_prob
                    )
                except OSError as error:
                    log_event(
                        logger, run_id, router_name, "DROP",
                        reason="send_error",
                        error=str(error),
                        next_hop=next_hop,
                        delay_ms=delay_ms,
                        loss_prob=loss_prob,
                        msg_id=msg_id,
                        msg_type=msg_type,
                        src_ip=src_ip,
                        dst_ip=dst_ip
                    )

            if delay_ms > 0:
                timer = threading.Timer(
                    delay_ms / 1000.0,
                    forward_packet
                )
                timer.daemon = True
                timer.start()
            else:
                forward_packet()


if __name__ == "__main__":
    args = sys.argv[1:]

    drop_first_data = False
    drop_final_ack = False
    corrupt_first_data = False
    seed = None

    # Accept all deterministic test flags in either order.
    remaining_args = []
    for arg in args:
        if arg == "--drop-first-data":
            drop_first_data = True
        elif arg == "--drop-final-ack":
            drop_final_ack = True
        elif arg == "--corrupt-first-data":
            corrupt_first_data = True
        else:
            remaining_args.append(arg)
    args = remaining_args

    if len(args) not in (1, 3) or (
        len(args) == 3 and args[1] != "--seed"
    ):
        print(
            "Usage: python router.py ROUTER_NAME "
            "[--seed INTEGER] [--drop-first-data] "
            "[--drop-final-ack] [--corrupt-first-data]"
        )
        print("Example: python router.py R_B --corrupt-first-data")
        sys.exit(1)

    try:
        seed = int(args[2]) if len(args) == 3 else None

        run_router(
            args[0],
            seed=seed,
            drop_first_data=drop_first_data,
            drop_final_ack=drop_final_ack,
            corrupt_first_data=corrupt_first_data
        )

    except KeyboardInterrupt:
        print("\nRouter stopped.")
    except (RuntimeError, ValueError, OSError) as error:
        print(f"Router startup/runtime error: {error}")
        sys.exit(1)
