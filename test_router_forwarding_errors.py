import json
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from packet import encode_packet


PROJECT_DIR = Path(__file__).resolve().parent
ROUTER_PORT = 15002
ROUTER_B_PORT = 15003
ROUTER_D_PORT = 15004

TESTS = [
    ("invalid_next_hop", "R_X"),
    ("next_hop_not_adjacent", "R_D"),
]


def run_test(expected_reason, next_hop):
    with tempfile.TemporaryDirectory(prefix="router_error_test_") as temp:
        temp_dir = Path(temp)
        config_path = temp_dir / "config.json"

        config = {
            "nodes": {
                "R_A": {
                    "virtual_ip": "10.0.0.2",
                    "udp_port": ROUTER_PORT,
                    "type": "router"
                },
                "R_B": {
                    "virtual_ip": "10.0.0.3",
                    "udp_port": ROUTER_B_PORT,
                    "type": "router"
                },
                "R_D": {
                    "virtual_ip": "10.0.0.5",
                    "udp_port": ROUTER_D_PORT,
                    "type": "router"
                }
            },
            "links": [
                {
                    "from": "R_A",
                    "to": "R_B",
                    "delay_ms": 0,
                    "loss_prob": 0.0
                }
            ],
            "forwarding_tables": {
                "R_A": {
                    "10.0.0.6": next_hop
                }
            }
        }

        config_path.write_text(
            json.dumps(config, indent=2),
            encoding="utf-8"
        )

        child_code = (
            "import sys; "
            f"sys.path.insert(0, {str(PROJECT_DIR)!r}); "
            "import router; "
            f"router.CONFIG_FILE = {str(config_path)!r}; "
            "router.run_router('R_A')"
        )

        log_dir = temp_dir / "logs"
        process = None

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiver_b, \
             socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiver_d:

            receiver_b.bind(("127.0.0.1", ROUTER_B_PORT))
            receiver_d.bind(("127.0.0.1", ROUTER_D_PORT))
            receiver_b.settimeout(0.2)
            receiver_d.settimeout(0.2)

            try:
                process = subprocess.Popen(
                    [sys.executable, "-c", child_code],
                    cwd=temp_dir,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )

                log_path = log_dir / "R_A.log"
                ready = False

                for _ in range(50):
                    if process.poll() is not None:
                        break
                    if log_path.exists():
                        content = log_path.read_text(encoding="utf-8")
                        if '"event": "LISTEN"' in content:
                            ready = True
                            break
                    time.sleep(0.1)

                if not ready:
                    print(f"FAIL: Router did not start for {expected_reason}.")
                    return False

                packet = encode_packet(
                    src_ip="10.0.0.1",
                    dst_ip="10.0.0.6",
                    hop_limit=10,
                    msg_type="PROBE",
                    msg_id=2001,
                    payload_str="Forwarding error acceptance test"
                )

                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender:
                    sender.sendto(packet, ("127.0.0.1", ROUTER_PORT))

                time.sleep(0.3)

                records = [
                    json.loads(line)
                    for line in log_path.read_text(encoding="utf-8").splitlines()
                    if line.strip()
                ]

                expected_drop = any(
                    record.get("event") == "DROP"
                    and record.get("reason") == expected_reason
                    for record in records
                )

                forwarded = any(
                    record.get("event") == "FORWARD"
                    for record in records
                )

                received_at_next_hop = False

                for receiver in (receiver_b, receiver_d):
                    try:
                        receiver.recvfrom(2048)
                        received_at_next_hop = True
                    except socket.timeout:
                        pass

                passed = (
                    expected_drop
                    and not forwarded
                    and not received_at_next_hop
                )

                print(
                    f"{'PASS' if passed else 'FAIL'}: {expected_reason}"
                )
                print(f"  Expected DROP logged: {expected_drop}")
                print(f"  Forward event recorded: {forwarded}")
                print(f"  Packet reached a next-hop endpoint: {received_at_next_hop}")

                return passed

            finally:
                if process is not None and process.poll() is None:
                    process.terminate()
                    process.wait(timeout=3)


def main():
    results = []

    for reason, next_hop in TESTS:
        results.append(run_test(reason, next_hop))

    print("\nFORWARDING ERROR TEST SUMMARY")
    print(f"Passed: {sum(results)}/{len(results)}")

    if all(results):
        print("SUCCESS: Both forwarding error checks passed.")
    else:
        print("FAILURE: Review the test output above.")


if __name__ == "__main__":
    main()
