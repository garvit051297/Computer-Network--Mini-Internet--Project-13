
import socket

from packet import encode_packet, decode_packet, MAX_PACKET_SIZE


ROUTER_A_ENDPOINT = ("127.0.0.1", 5002)
ROUTER_B_ENDPOINT = ("127.0.0.1", 5003)

TEST_MESSAGE_ID = 2001
TEST_PAYLOAD = "Forwarding test from H1 to H2"


def main():
    # Temporarily act as the receiver at Router B's UDP endpoint.
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiver:
        receiver.bind(ROUTER_B_ENDPOINT)
        receiver.settimeout(5.0)

        print("Test receiver listening on UDP port 5003.")

        # Use a separate socket to send a packet to Router A.
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender:
            packet = encode_packet(
                src_ip="10.0.0.1",
                dst_ip="10.0.0.6",
                hop_limit=10,
                msg_type="PROBE",
                msg_id=TEST_MESSAGE_ID,
                payload_str=TEST_PAYLOAD
            )

            print("Sending test packet to Router A on port 5002...")
            sender.sendto(packet, ROUTER_A_ENDPOINT)

        try:
            data, source = receiver.recvfrom(MAX_PACKET_SIZE + 1)
        except socket.timeout:
            print("FAIL: No forwarded packet received within 5 seconds.")
            return

        received = decode_packet(data)

        if received is None:
            print("FAIL: Received a malformed or oversized packet.")
            return

        print("Received packet from:", source)
        print("Decoded packet:", received)

        checks = {
            "Correct source virtual IP": received["src_ip"] == "10.0.0.1",
            "Correct destination virtual IP": received["dst_ip"] == "10.0.0.6",
            "Correct message type": received["msg_type"] == "PROBE",
            "Correct message ID": received["msg_id"] == TEST_MESSAGE_ID,
            "Payload preserved": received["payload"] == TEST_PAYLOAD,
            "Hop limit decremented": received["hop_limit"] == 9
        }

        print("\nVerification results:")
        for description, passed in checks.items():
            print(f"{'PASS' if passed else 'FAIL'}: {description}")

        if all(checks.values()) and source[1] == 5002:
            print("\nSUCCESS: Router A forwarded the packet to Router B's endpoint.")
        else:
            print("\nFAIL: One or more forwarding checks did not pass.")


if __name__ == "__main__":
    main()
