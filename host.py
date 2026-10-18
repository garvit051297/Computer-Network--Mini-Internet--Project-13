
import json
import socket
import sys
import time
from datetime import datetime, timezone

from packet import encode_packet, decode_packet, MAX_PACKET_SIZE


CONFIG_FILE = "config.json"
RECEIVE_BUFFER_SIZE = MAX_PACKET_SIZE + 1
DEFAULT_HOP_LIMIT = 10
PROBE_TIMEOUT_SECONDS = 8.0


def log_event(host_name, event, **details):
    """Print a timestamped host event."""
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device": host_name,
        "event": event
    }
    record.update(details)
    print(json.dumps(record), flush=True)


def load_config():
    """Load and validate the network configuration."""
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as file:
            config = json.load(file)
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Could not load {CONFIG_FILE}: {error}")

    for key in ("nodes", "links"):
        if key not in config:
            raise ValueError(f"Missing '{key}' in {CONFIG_FILE}")

    return config


def get_gateway(config, host_name):
    """Return the configured default gateway after checking adjacency."""
    nodes = config["nodes"]
    host = nodes[host_name]
    gateway_name = host.get("default_gateway")

    if not gateway_name or gateway_name not in nodes:
        raise ValueError(f"Invalid default gateway for {host_name}")

    gateway = nodes[gateway_name]

    if gateway.get("type") != "router":
        raise ValueError(f"{gateway_name} is not a router")

    connected = any(
        {link.get("from"), link.get("to")} == {host_name, gateway_name}
        for link in config["links"]
    )

    if not connected:
        raise ValueError(
            f"{host_name} and gateway {gateway_name} "
            "are not connected in config.json"
        )

    return gateway_name, gateway


def run_host(host_name):
    config = load_config()
    nodes = config["nodes"]

    if host_name not in nodes:
        raise ValueError(f"Unknown node: {host_name}")

    host = nodes[host_name]

    if host.get("type") != "host":
        raise ValueError(f"{host_name} is not configured as a host")

    gateway_name, gateway = get_gateway(config, host_name)

    host_ip = host["virtual_ip"]
    host_port = host["udp_port"]
    gateway_ip = gateway["virtual_ip"]
    gateway_port = gateway["udp_port"]

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        # Bind to this host's configured UDP endpoint.
        sock.bind(("127.0.0.1", host_port))

        log_event(
            host_name,
            "LISTEN",
            virtual_ip=host_ip,
            udp_endpoint=f"127.0.0.1:{host_port}",
            default_gateway=gateway_name
        )

        if host_name == "H2":
            run_echo_server(
                sock, host_name, host_ip, gateway_name,
                gateway_ip, gateway_port
            )
        elif host_name == "H1":
            run_probe_client(
                sock, host_name, host_ip, gateway_name,
                gateway_ip, gateway_port
            )
        else:
            raise ValueError(
                f"No host application behavior defined for {host_name}"
            )


def run_echo_server(
    sock, host_name, host_ip, gateway_name, gateway_ip, gateway_port
):
    """Receive probes at H2 and send echoes through H2's gateway."""
    while True:
        data, source_endpoint = sock.recvfrom(RECEIVE_BUFFER_SIZE)
        packet = decode_packet(data)

        if packet is None:
            log_event(host_name, "DROP", reason="malformed_or_oversized_packet")
            continue

        log_event(
            host_name,
            "RECEIVE",
            msg_id=packet["msg_id"],
            msg_type=packet["msg_type"],
            src_ip=packet["src_ip"],
            dst_ip=packet["dst_ip"],
            hop_limit=packet["hop_limit"],
            udp_source=f"{source_endpoint[0]}:{source_endpoint[1]}"
        )

        if packet["dst_ip"] != host_ip:
            log_event(
                host_name, "DROP",
                reason="packet_not_addressed_to_this_host",
                msg_id=packet["msg_id"],
                dst_ip=packet["dst_ip"]
            )
            continue

        if packet["msg_type"] != "PROBE":
            log_event(
                host_name, "DROP",
                reason="unsupported_message_type",
                msg_id=packet["msg_id"],
                msg_type=packet["msg_type"]
            )
            continue

        # The echo is a new packet addressed to the original source.
        # Send it to H2's configured gateway, not directly to H1.
        try:
            echo = encode_packet(
                src_ip=host_ip,
                dst_ip=packet["src_ip"],
                hop_limit=DEFAULT_HOP_LIMIT,
                msg_type="ECHO",
                msg_id=packet["msg_id"],
                payload_str=packet["payload"]
            )
        except (ValueError, TypeError) as error:
            log_event(
                host_name, "DROP",
                reason="echo_encoding_error",
                error=str(error),
                msg_id=packet["msg_id"]
            )
            continue

        sock.sendto(echo, ("127.0.0.1", gateway_port))

        log_event(
            host_name,
            "SEND_ECHO",
            msg_id=packet["msg_id"],
            src_ip=host_ip,
            dst_ip=packet["src_ip"],
            next_hop=gateway_name,
            udp_destination=f"127.0.0.1:{gateway_port}"
        )


def run_probe_client(
    sock, host_name, host_ip, gateway_name, gateway_ip, gateway_port
):
    """Send one probe and wait for its matching echo."""
    time.sleep(0.3)

    msg_id = int(time.time() * 1000) % 4294967296
    payload = "Hello Mini Internet!"

    probe = encode_packet(
        src_ip=host_ip,
        dst_ip="10.0.0.6",
        hop_limit=DEFAULT_HOP_LIMIT,
        msg_type="PROBE",
        msg_id=msg_id,
        payload_str=payload
    )

    sock.settimeout(PROBE_TIMEOUT_SECONDS)

    log_event(
        host_name,
        "SEND_PROBE",
        msg_id=msg_id,
        src_ip=host_ip,
        dst_ip="10.0.0.6",
        next_hop=gateway_name,
        udp_destination=f"127.0.0.1:{gateway_port}"
    )

    sock.sendto(probe, ("127.0.0.1", gateway_port))

    deadline = time.monotonic() + PROBE_TIMEOUT_SECONDS

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            log_event(
                host_name, "TIMEOUT",
                msg_id=msg_id,
                reason="no_matching_echo_received"
            )
            return

        sock.settimeout(remaining)

        try:
            data, source_endpoint = sock.recvfrom(RECEIVE_BUFFER_SIZE)
        except socket.timeout:
            log_event(
                host_name, "TIMEOUT",
                msg_id=msg_id,
                reason="no_matching_echo_received"
            )
            return

        response = decode_packet(data)

        if response is None:
            log_event(
                host_name, "DROP",
                reason="malformed_or_oversized_response"
            )
            continue

        log_event(
            host_name,
            "RECEIVE",
            msg_id=response["msg_id"],
            msg_type=response["msg_type"],
            src_ip=response["src_ip"],
            dst_ip=response["dst_ip"],
            hop_limit=response["hop_limit"],
            udp_source=f"{source_endpoint[0]}:{source_endpoint[1]}"
        )

        if (
            response["msg_type"] == "ECHO"
            and response["src_ip"] == "10.0.0.6"
            and response["dst_ip"] == host_ip
            and response["msg_id"] == msg_id
            and response["payload"] == payload
        ):
            log_event(
                host_name,
                "SUCCESS",
                result="matching_echo_received",
                msg_id=msg_id
            )
            print("SUCCESS: Matching ECHO received.", flush=True)
            return

        log_event(
            host_name,
            "IGNORE",
            reason="response_did_not_match_probe",
            expected_msg_id=msg_id,
            received_msg_id=response["msg_id"]
        )


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python host.py HOST_NAME")
        print("Examples: python host.py H1")
        print("          python host.py H2")
        sys.exit(1)

    try:
        run_host(sys.argv[1])
    except KeyboardInterrupt:
        print("\nHost stopped.")
    except (RuntimeError, ValueError, OSError) as error:
        print(f"Host startup/runtime error: {error}")
        sys.exit(1)
