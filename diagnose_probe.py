
import json
import random
import socket
import time

from packet import encode_packet, decode_packet, MAX_PACKET_SIZE

HOST_IP = "10.0.0.1"
DEST_IP = "10.0.0.6"
HOST_PORT = 5001
GATEWAY_PORT = 5002
HOP_LIMIT = 10
PAYLOAD = "Mini Internet baseline experiment"
TIMEOUT_SECONDS = 5.0


def main():
    msg_id = random.SystemRandom().randrange(0, 2**32)
    packet = encode_packet(
        src_ip=HOST_IP,
        dst_ip=DEST_IP,
        hop_limit=HOP_LIMIT,
        msg_type="PROBE",
        msg_id=msg_id,
        payload_str=PAYLOAD,
    )

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", HOST_PORT))
        sock.settimeout(TIMEOUT_SECONDS)

        sent_at = time.monotonic()
        sock.sendto(packet, ("127.0.0.1", GATEWAY_PORT))
        print(f"Sent diagnostic probe: msg_id={msg_id}")

        try:
            while True:
                data, endpoint = sock.recvfrom(MAX_PACKET_SIZE + 1)
                received_at = time.monotonic()
                response = decode_packet(data)

                if response is None:
                    continue

                matches = (
                    endpoint == ("127.0.0.1", GATEWAY_PORT)
                    and response["msg_type"] == "ECHO"
                    and response["msg_id"] == msg_id
                    and response["src_ip"] == DEST_IP
                    and response["dst_ip"] == HOST_IP
                    and response["payload"] == PAYLOAD
                )

                if matches:
                    print("SUCCESS: Matching ECHO received.")
                    print(f"UDP source: {endpoint}")
                    print(f"RTT: {(received_at - sent_at) * 1000:.3f} ms")
                    return

        except socket.timeout:
            print("FAILURE: No matching ECHO received within 5 seconds.")
            raise SystemExit(1)


if __name__ == "__main__":
    main()
