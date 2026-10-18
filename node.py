import json
import socket
import threading
import time

from packet import encode_packet, decode_packet


CONFIG_FILE = "config.json"
MAX_PACKET_SIZE = 1200


def load_config():
    """Load and validate the configuration needed for this test."""

    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as file:
            config = json.load(file)
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Could not load {CONFIG_FILE}: {error}")

    nodes = config.get("nodes", {})
    links = config.get("links", [])

    for name in ("H1", "R_A"):
        if name not in nodes:
            raise ValueError(f"Missing node in config.json: {name}")

        port = nodes[name].get("udp_port")
        if not isinstance(port, int) or not 1 <= port <= 65535:
            raise ValueError(f"Invalid UDP port for {name}")

        if not nodes[name].get("virtual_ip"):
            raise ValueError(f"Missing virtual IP for {name}")

    connected = any(
        (link.get("from") == "H1" and link.get("to") == "R_A")
        or (link.get("from") == "R_A" and link.get("to") == "H1")
        for link in links
    )

    if not connected:
        raise ValueError("H1 and R_A are not connected in config.json")

    return config


def receiver_thread(config):
    """Simulate Router A receiving a PROBE and returning an ECHO."""

    router = config["nodes"]["R_A"]
    router_ip = router["virtual_ip"]
    router_port = router["udp_port"]

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", router_port))
        print(f"[Router A] Listening on UDP port {router_port}...")

        while True:
            data, addr = sock.recvfrom(MAX_PACKET_SIZE)
            packet = decode_packet(data)

            if packet is None:
                print("[Router A] Dropped malformed packet.")
                continue

            if packet["dst_ip"] != router_ip:
                print("[Router A] Dropped packet not addressed to Router A.")
                continue

            print(f"[Router A] Received from {addr}: {packet}")

            if packet["msg_type"] == "PROBE":
                echo_packet = encode_packet(
                    src_ip=router_ip,
                    dst_ip=packet["src_ip"],
                    hop_limit=packet["hop_limit"],
                    msg_type="ECHO",
                    msg_id=packet["msg_id"],
                    payload_str=packet["payload"]
                )

                sock.sendto(echo_packet, addr)
                print(f"[Router A] Sent ECHO back to {addr}")


def sender(config):
    """Simulate Host 1 sending a PROBE to Router A."""

    host = config["nodes"]["H1"]
    router = config["nodes"]["R_A"]

    time.sleep(0.5)

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(2.0)

        probe = encode_packet(
            src_ip=host["virtual_ip"],
            dst_ip=router["virtual_ip"],
            hop_limit=10,
            msg_type="PROBE",
            msg_id=999,
            payload_str="Hello Mini Internet!"
        )

        destination = ("127.0.0.1", router["udp_port"])

        print("[Host 1] Sending PROBE to Router A...")
        sock.sendto(probe, destination)

        try:
            data, addr = sock.recvfrom(MAX_PACKET_SIZE)
            response = decode_packet(data)

            if response is None:
                print("[Host 1] Received a malformed response.")
                return

            print(f"[Host 1] Received response: {response}")

            if (
                response["msg_type"] == "ECHO"
                and response["src_ip"] == router["virtual_ip"]
                and response["dst_ip"] == host["virtual_ip"]
                and response["msg_id"] == 999
                and response["payload"] == "Hello Mini Internet!"
            ):
                print("[Host 1] SUCCESS: Matching ECHO received.")
            else:
                print("[Host 1] ERROR: Response did not match the PROBE.")

        except socket.timeout:
            print("[Host 1] Timed out waiting for response.")


if __name__ == "__main__":
    try:
        config = load_config()

        thread = threading.Thread(
            target=receiver_thread,
            args=(config,),
            daemon=True
        )
        thread.start()

        sender(config)

    except (RuntimeError, ValueError, OSError) as error:
        print(f"Startup error: {error}")