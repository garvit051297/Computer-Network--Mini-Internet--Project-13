
import io
import json
import socket
import unittest
from contextlib import redirect_stdout

from packet import encode_packet, decode_packet
from reliable_protocol import encode_data, encode_ack
from reliable_transfer import run_reliable_sender, run_reliable_receiver


HOST_IP = "10.0.0.1"
RECEIVER_IP = "10.0.0.6"
GATEWAY_NAME = "R_A"
GATEWAY_PORT = 5002
RECEIVER_GATEWAY_IP = "10.0.0.5"
RECEIVER_GATEWAY_PORT = 5005


class FakeSocket:
    def __init__(self, responses=None):
        self.responses = list(responses or [])
        self.sent = []
        self.timeout = None

    def settimeout(self, value):
        self.timeout = value

    def sendto(self, data, address):
        self.sent.append((data, address))
        return len(data)

    def recvfrom(self, buffer_size):
        if not self.responses:
            raise socket.timeout()

        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def packet_fields(raw):
    """Return a decoded packet or fail clearly if it is invalid."""
    packet = decode_packet(raw)
    if packet is None:
        raise AssertionError("Expected a valid encoded packet.")
    return packet


def make_ack_packet(msg_id, transfer_id, seq):
    payload = encode_ack(transfer_id, seq)
    raw = encode_packet(
        RECEIVER_IP,
        HOST_IP,
        10,
        "ACK",
        msg_id,
        payload,
    )
    return raw, ("127.0.0.1", 5002)


def make_data_packet(msg_id, transfer_id, seq, data, is_last):
    payload = encode_data(transfer_id, seq, data, is_last)
    raw = encode_packet(
        HOST_IP,
        RECEIVER_IP,
        10,
        "DATA",
        msg_id,
        payload,
    )
    return raw, ("127.0.0.1", 5002)


def make_corrupted_data_packet(msg_id, transfer_id, seq, data, is_last):
    """Create DATA with modified content but its original CRC-32."""
    raw, address = make_data_packet(
        msg_id, transfer_id, seq, data, is_last
    )

    packet = packet_fields(raw)
    message = json.loads(packet["payload"])

    # Modify application data without recalculating the CRC-32.
    message["data"] = "CORRUPTED:" + message["data"]

    corrupted_payload = json.dumps(
        message,
        ensure_ascii=False,
        separators=(",", ":")
    )

    corrupted_raw = encode_packet(
        HOST_IP,
        RECEIVER_IP,
        10,
        "DATA",
        msg_id,
        corrupted_payload,
    )

    return corrupted_raw, address


class ReliableSenderTests(unittest.TestCase):

    def test_sender_fails_boundedly_when_no_ack_arrives(self):
        fake = FakeSocket()

        with redirect_stdout(io.StringIO()):
            result = run_reliable_sender(
                sock=fake,
                host_ip=HOST_IP,
                gateway_name=GATEWAY_NAME,
                gateway_port=GATEWAY_PORT,
                destination_ip=RECEIVER_IP,
                messages=["unreachable"],
                ack_timeout=0.01,
                max_retries=2,
                transfer_id="test-transfer-failure",
            )

        self.assertFalse(result["success"])
        self.assertEqual(len(fake.sent), 3)
        self.assertEqual(result["retransmissions"], 2)

    def test_sender_retransmits_after_lost_ack(self):
        transfer_id = "test-transfer-retry"

        class RetrySocket(FakeSocket):
            def __init__(self):
                super().__init__()
                self.receive_calls = 0

            def recvfrom(self, buffer_size):
                self.receive_calls += 1

                # Simulate the first ACK being lost.
                if self.receive_calls == 1:
                    raise socket.timeout()

                sent_packet = packet_fields(self.sent[-1][0])
                return make_ack_packet(
                    sent_packet["msg_id"],
                    transfer_id,
                    0,
                )

        fake = RetrySocket()

        with redirect_stdout(io.StringIO()):
            result = run_reliable_sender(
                sock=fake,
                host_ip=HOST_IP,
                gateway_name=GATEWAY_NAME,
                gateway_port=GATEWAY_PORT,
                destination_ip=RECEIVER_IP,
                messages=["retry me"],
                ack_timeout=0.01,
                max_retries=2,
                transfer_id=transfer_id,
            )

        self.assertTrue(result["success"])
        self.assertEqual(result["retransmissions"], 1)
        self.assertEqual(len(fake.sent), 2)

        first = packet_fields(fake.sent[0][0])
        second = packet_fields(fake.sent[1][0])
        self.assertEqual(first["msg_id"], second["msg_id"])
        self.assertEqual(first["payload"], second["payload"])

    def test_sender_completes_two_messages_in_order(self):
        transfer_id = "test-transfer-two-messages"

        class TwoMessageSocket(FakeSocket):
            def recvfrom(self, buffer_size):
                sent_packet = packet_fields(self.sent[-1][0])
                payload = json.loads(sent_packet["payload"])
                ack = make_ack_packet(
                    sent_packet["msg_id"],
                    transfer_id,
                    payload["seq"],
                )
                return ack

        fake = TwoMessageSocket()

        with redirect_stdout(io.StringIO()):
            result = run_reliable_sender(
                sock=fake,
                host_ip=HOST_IP,
                gateway_name=GATEWAY_NAME,
                gateway_port=GATEWAY_PORT,
                destination_ip=RECEIVER_IP,
                messages=["first", "second"],
                ack_timeout=0.01,
                max_retries=2,
                transfer_id=transfer_id,
            )

        self.assertTrue(result["success"])
        self.assertEqual(result["retransmissions"], 0)
        self.assertEqual(len(fake.sent), 2)

        sent_payloads = [
            json.loads(packet_fields(raw)["payload"])
            for raw, _address in fake.sent
        ]
        self.assertEqual(
            [payload["seq"] for payload in sent_payloads],
            [0, 1],
        )
        self.assertEqual(
            [payload["data"] for payload in sent_payloads],
            ["first", "second"],
        )


class ReliableReceiverTests(unittest.TestCase):

    def test_receiver_delivers_once_and_reacks_duplicate_final(self):
        transfer_id = "test-receiver-transfer"

        responses = [
            make_data_packet(101, transfer_id, 0, "first", False),
            make_data_packet(102, transfer_id, 1, "second", True),
            # Simulate retransmission after the final ACK was lost.
            make_data_packet(102, transfer_id, 1, "second", True),
            socket.timeout(),
        ]

        fake = FakeSocket(responses)

        with redirect_stdout(io.StringIO()) as captured:
            run_reliable_receiver(
                sock=fake,
                host_ip=RECEIVER_IP,
                gateway_name="R_D",
                gateway_ip=RECEIVER_GATEWAY_IP,
                gateway_port=RECEIVER_GATEWAY_PORT,
                expected_sender_ip=HOST_IP,
                grace_seconds=0.01,
            )

        delivered_lines = [
            line for line in captured.getvalue().splitlines()
            if "DELIVER_DATA" in line
        ]
        self.assertEqual(len(delivered_lines), 2)

        ack_packets = []
        for raw, _address in fake.sent:
            packet = packet_fields(raw)
            if packet["msg_type"] == "ACK":
                ack_packets.append(packet)

        self.assertEqual(len(ack_packets), 3)

        ack_payloads = [
            json.loads(packet["payload"])
            for packet in ack_packets
        ]
        self.assertEqual(
            [payload["seq"] for payload in ack_payloads],
            [0, 1, 1],
        )

    def test_receiver_rejects_corrupted_data_and_accepts_retransmission(self):
        transfer_id = "test-crc32-recovery"

        # First arrival has corrupted application data and an unchanged CRC.
        corrupted_packet = make_corrupted_data_packet(
            201, transfer_id, 0, "Important message", True
        )

        # Retransmission contains the original, valid DATA payload.
        valid_packet = make_data_packet(
            201, transfer_id, 0, "Important message", True
        )

        responses = [
            corrupted_packet,
            valid_packet,
            socket.timeout(),
        ]

        fake = FakeSocket(responses)

        with redirect_stdout(io.StringIO()) as captured:
            run_reliable_receiver(
                sock=fake,
                host_ip=RECEIVER_IP,
                gateway_name="R_D",
                gateway_ip=RECEIVER_GATEWAY_IP,
                gateway_port=RECEIVER_GATEWAY_PORT,
                expected_sender_ip=HOST_IP,
                grace_seconds=0.01,
            )

        output = captured.getvalue()

        # The corrupted DATA must be identified and rejected.
        self.assertIn("checksum_mismatch", output)

        # The valid retransmission must be delivered exactly once.
        delivered_lines = [
            line for line in output.splitlines()
            if "DELIVER_DATA" in line
        ]
        self.assertEqual(len(delivered_lines), 1)
        self.assertIn("Important message", delivered_lines[0])

        # Only the valid retransmission should receive an ACK.
        ack_packets = []
        for raw, _address in fake.sent:
            packet = packet_fields(raw)
            if packet["msg_type"] == "ACK":
                ack_packets.append(packet)

        self.assertEqual(len(ack_packets), 1)

        ack_payload = json.loads(ack_packets[0]["payload"])
        self.assertEqual(ack_payload["transfer_id"], transfer_id)
        self.assertEqual(ack_payload["seq"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
