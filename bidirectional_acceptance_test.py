import csv
import json
import select
import socket
import time
from datetime import datetime, timezone
from pathlib import Path

from packet import encode_packet, decode_packet, MAX_PACKET_SIZE


CONFIG_FILE = "config.json"
RESULTS_FILE = Path("results") / "bidirectional_acceptance_results.csv"

PROBE_COUNT = 20
HOP_LIMIT = 10
TIMEOUT_SECONDS = 3.0
INTER_PROBE_INTERVAL_SECONDS = 0.3
RECEIVE_BUFFER_SIZE = MAX_PACKET_SIZE + 1


def load_config():
    with open(CONFIG_FILE, "r", encoding="utf-8") as file:
        return json.load(file)


def validate_config(config):
    for link in config["links"]:
        if link["loss_prob"] != 0.0:
            raise ValueError("All links must have zero packet loss.")

    for host_name in ("H1", "H2"):
        host = config["nodes"][host_name]
        gateway_name = host["default_gateway"]

        if gateway_name not in config["nodes"]:
            raise ValueError(f"Invalid gateway for {host_name}.")

        connected = any(
            {link["from"], link["to"]} == {host_name, gateway_name}
            for link in config["links"]
        )

        if not connected:
            raise ValueError(f"{host_name} is not connected to its gateway.")


def get_host_info(config, host_name):
    host = config["nodes"][host_name]
    gateway = config["nodes"][host["default_gateway"]]

    return {
        "ip": host["virtual_ip"],
        "port": host["udp_port"],
        "gateway_port": gateway["udp_port"],
    }


def send_echo(receiver_sock, receiver_name, packet, hosts):
    sender_name = next(
        (
            name for name, info in hosts.items()
            if info["ip"] == packet["src_ip"]
        ),
        None,
    )

    if sender_name is None:
        return

    receiver_ip = hosts[receiver_name]["ip"]

    echo = encode_packet(
        src_ip=receiver_ip,
        dst_ip=packet["src_ip"],
        hop_limit=HOP_LIMIT,
        msg_type="ECHO",
        msg_id=packet["msg_id"],
        payload_str=packet["payload"],
    )

    sender_gateway_port = hosts[sender_name]["gateway_port"]

    receiver_sock.sendto(
        echo,
        ("127.0.0.1", sender_gateway_port),
    )


def run_direction(sender_name, receiver_name, sockets, hosts, results):
    sender = hosts[sender_name]
    receiver = hosts[receiver_name]

    sender_sock = sockets[sender_name]
    receiver_sock = sockets[receiver_name]

    successes = 0
    timeouts = 0
    rtts = []

    print()
    print("=" * 65)
    print(f"TEST: {sender_name} -> {receiver_name}")
    print("=" * 65)

    for sequence in range(1, PROBE_COUNT + 1):
        msg_id = int(time.time_ns() % 4294967296)

        payload = (
            f"BIDIRECTIONAL_ACCEPTANCE|"
            f"{sender_name}|{receiver_name}|{sequence}"
        )

        probe = encode_packet(
            src_ip=sender["ip"],
            dst_ip=receiver["ip"],
            hop_limit=HOP_LIMIT,
            msg_type="PROBE",
            msg_id=msg_id,
            payload_str=payload,
        )

        start = time.monotonic()

        sender_sock.sendto(
            probe,
            ("127.0.0.1", sender["gateway_port"]),
        )

        deadline = start + TIMEOUT_SECONDS
        received = False

        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()

            readable, _, _ = select.select(
                [sender_sock, receiver_sock],
                [],
                [],
                remaining,
            )

            if not readable:
                break

            for active_sock in readable:
                data, source_endpoint = active_sock.recvfrom(
                    RECEIVE_BUFFER_SIZE
                )

                packet = decode_packet(data)

                if packet is None:
                    continue

                if active_sock is receiver_sock:
                    if (
                        packet["msg_type"] == "PROBE"
                        and packet["dst_ip"] == receiver["ip"]
                        and packet["src_ip"] == sender["ip"]
                    ):
                        send_echo(
                            receiver_sock,
                            receiver_name,
                            packet,
                            hosts,
                        )

                elif (
                    packet["msg_type"] == "ECHO"
                    and packet["src_ip"] == receiver["ip"]
                    and packet["dst_ip"] == sender["ip"]
                    and packet["msg_id"] == msg_id
                    and packet["payload"] == payload
                ):
                    rtt_ms = (
                        time.monotonic() - start
                    ) * 1000.0

                    rtts.append(rtt_ms)
                    successes += 1
                    received = True

                    print(
                        f"{sequence:02d}/{PROBE_COUNT}: SUCCESS "
                        f"| ID={msg_id} | RTT={rtt_ms:.3f} ms"
                    )

                    results.append({
                        "timestamp": datetime.now(
                            timezone.utc
                        ).isoformat(),
                        "sender": sender_name,
                        "receiver": receiver_name,
                        "sequence": sequence,
                        "msg_id": msg_id,
                        "status": "SUCCESS",
                        "rtt_ms": round(rtt_ms, 3),
                        "payload": payload,
                    })

                    break

            if received:
                break

        if not received:
            timeouts += 1

            print(
                f"{sequence:02d}/{PROBE_COUNT}: TIMEOUT "
                f"| ID={msg_id}"
            )

            results.append({
                "timestamp": datetime.now(
                    timezone.utc
                ).isoformat(),
                "sender": sender_name,
                "receiver": receiver_name,
                "sequence": sequence,
                "msg_id": msg_id,
                "status": "TIMEOUT",
                "rtt_ms": "",
                "payload": payload,
            })

        time.sleep(INTER_PROBE_INTERVAL_SECONDS)

    average_rtt = sum(rtts) / len(rtts) if rtts else 0.0

    print("-" * 65)
    print(f"Successful: {successes}/{PROBE_COUNT}")
    print(f"Timeouts:   {timeouts}")
    print(f"Average RTT: {average_rtt:.3f} ms")
    print("-" * 65)

    return successes, timeouts, average_rtt


def main():
    config = load_config()
    validate_config(config)

    hosts = {
        name: get_host_info(config, name)
        for name in ("H1", "H2")
    }

    RESULTS_FILE.parent.mkdir(exist_ok=True)

    sockets = {}
    results = []

    try:
        for host_name in ("H1", "H2"):
            sock = socket.socket(
                socket.AF_INET,
                socket.SOCK_DGRAM,
            )

            sock.bind(
                ("127.0.0.1", hosts[host_name]["port"])
            )

            sockets[host_name] = sock

        print("BIDIRECTIONAL ACCEPTANCE TEST")
        print("Configured packet loss: 0%")
        print(f"Probes per direction: {PROBE_COUNT}")

        first = run_direction(
            "H1", "H2", sockets, hosts, results
        )

        second = run_direction(
            "H2", "H1", sockets, hosts, results
        )

    finally:
        for sock in sockets.values():
            sock.close()

    with open(
        RESULTS_FILE,
        "w",
        newline="",
        encoding="utf-8",
    ) as file:
        fields = [
            "timestamp",
            "sender",
            "receiver",
            "sequence",
            "msg_id",
            "status",
            "rtt_ms",
            "payload",
        ]

        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(results)

    total_successes = first[0] + second[0]
    total_timeouts = first[1] + second[1]

    print()
    print("=" * 65)
    print("FINAL ACCEPTANCE TEST SUMMARY")
    print("=" * 65)
    print(f"H1 -> H2: {first[0]}/20")
    print(f"H2 -> H1: {second[0]}/20")
    print(f"Total:    {total_successes}/40")
    print(f"Timeouts: {total_timeouts}")
    print(f"Results:  {RESULTS_FILE}")

    if total_successes == 40:
        print("RESULT: BIDIRECTIONAL ACCEPTANCE TEST PASSED")
    else:
        print("RESULT: BIDIRECTIONAL ACCEPTANCE TEST NOT PASSED")


if __name__ == "__main__":
    main()