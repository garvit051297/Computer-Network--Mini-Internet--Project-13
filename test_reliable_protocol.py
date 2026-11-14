
import json
import unittest

from reliable_protocol import (
    encode_data,
    decode_data,
    decode_data_detailed,
    encode_ack,
    decode_ack,
)


class TestReliableProtocol(unittest.TestCase):

    def test_data_round_trip(self):
        payload = encode_data("transfer-001", 0, "Hello Mini Internet!")
        self.assertEqual(
            decode_data(payload),
            {
                "transfer_id": "transfer-001",
                "seq": 0,
                "data": "Hello Mini Internet!",
                "is_last": False,
            },
        )

    def test_data_round_trip_with_unicode(self):
        original = "Hello, 世界! 🌍"
        payload = encode_data("transfer-002", 1, original)
        decoded = decode_data(payload)

        self.assertIsNotNone(decoded)
        self.assertEqual(decoded["data"], original)
        self.assertEqual(decoded["seq"], 1)
        self.assertFalse(decoded["is_last"])

    def test_ack_round_trip(self):
        payload = encode_ack("transfer-001", 0)
        self.assertEqual(
            decode_ack(payload),
            {
                "transfer_id": "transfer-001",
                "seq": 0,
            },
        )

    def test_data_accepts_empty_application_data(self):
        payload = encode_data("transfer-empty", 0, "")
        decoded = decode_data(payload)

        self.assertIsNotNone(decoded)
        self.assertEqual(decoded["data"], "")
        self.assertFalse(decoded["is_last"])

    def test_data_rejects_malformed_json(self):
        self.assertIsNone(decode_data("{invalid json"))

    def test_ack_rejects_malformed_json(self):
        self.assertIsNone(decode_ack("{invalid json"))

    def test_data_rejects_missing_field(self):
        self.assertIsNone(
            decode_data('{"transfer_id":"t1","seq":0}')
        )

    def test_ack_rejects_missing_field(self):
        self.assertIsNone(
            decode_ack('{"transfer_id":"t1"}')
        )

    def test_data_rejects_extra_field(self):
        self.assertIsNone(
            decode_data(
                '{"transfer_id":"t1","seq":0,"data":"hello",'
                '"is_last":false,"extra":1}'
            )
        )

    def test_ack_rejects_extra_field(self):
        self.assertIsNone(
            decode_ack(
                '{"transfer_id":"t1","seq":0,"extra":1}'
            )
        )

    def test_data_rejects_negative_sequence(self):
        self.assertIsNone(
            decode_data(
                '{"transfer_id":"t1","seq":-1,"data":"hello",'
                '"is_last":false}'
            )
        )

    def test_ack_rejects_boolean_sequence(self):
        self.assertIsNone(
            decode_ack(
                '{"transfer_id":"t1","seq":true}'
            )
        )

    def test_data_rejects_non_string_data(self):
        self.assertIsNone(
            decode_data(
                '{"transfer_id":"t1","seq":0,"data":123,'
                '"is_last":false}'
            )
        )

    def test_encode_rejects_empty_transfer_id(self):
        with self.assertRaises(ValueError):
            encode_data("", 0, "hello")

    def test_encode_rejects_invalid_sequence(self):
        with self.assertRaises(ValueError):
            encode_ack("t1", -1)

    def test_encode_rejects_sequence_above_limit(self):
        with self.assertRaises(ValueError):
            encode_data("t1", 2**32, "hello")

    def test_data_rejects_oversized_payload(self):
        with self.assertRaises(ValueError):
            encode_data("t1", 0, "A" * 1200)

    def test_ack_rejects_transfer_id_over_character_limit(self):
        with self.assertRaises(ValueError):
            encode_ack("T" * 129, 0)

    def test_final_data_round_trip(self):
        payload = encode_data(
            "transfer-final", 3, "Final message", is_last=True
        )
        decoded = decode_data(payload)

        self.assertIsNotNone(decoded)
        self.assertEqual(decoded["transfer_id"], "transfer-final")
        self.assertEqual(decoded["seq"], 3)
        self.assertEqual(decoded["data"], "Final message")
        self.assertIs(decoded["is_last"], True)

    def test_data_rejects_missing_is_last(self):
        self.assertIsNone(
            decode_data(
                '{"transfer_id":"t1","seq":0,"data":"hello"}'
            )
        )

    def test_data_rejects_non_boolean_is_last(self):
        self.assertIsNone(
            decode_data(
                '{"transfer_id":"t1","seq":0,'
                '"data":"hello","is_last":1}'
            )
        )

    # CRC-32 integrity tests

    def test_crc32_rejects_modified_application_data(self):
        payload = encode_data(
            "transfer-crc-001", 0, "Original message"
        )

        message = json.loads(payload)
        message["data"] = "Modified message"

        corrupted_payload = json.dumps(
            message,
            ensure_ascii=False,
            separators=(",", ":")
        )

        decoded, error = decode_data_detailed(corrupted_payload)

        self.assertIsNone(decoded)
        self.assertEqual(error, "checksum_mismatch")

    def test_crc32_rejects_modified_sequence_number(self):
        payload = encode_data(
            "transfer-crc-002", 0, "Sequence test"
        )

        message = json.loads(payload)
        message["seq"] = 1

        corrupted_payload = json.dumps(
            message,
            ensure_ascii=False,
            separators=(",", ":")
        )

        decoded, error = decode_data_detailed(corrupted_payload)

        self.assertIsNone(decoded)
        self.assertEqual(error, "checksum_mismatch")

    def test_crc32_rejects_modified_transfer_id(self):
        payload = encode_data(
            "transfer-crc-003", 0, "Transfer ID test"
        )

        message = json.loads(payload)
        message["transfer_id"] = "modified-transfer"

        corrupted_payload = json.dumps(
            message,
            ensure_ascii=False,
            separators=(",", ":")
        )

        decoded, error = decode_data_detailed(corrupted_payload)

        self.assertIsNone(decoded)
        self.assertEqual(error, "checksum_mismatch")

    def test_crc32_rejects_modified_is_last_flag(self):
        payload = encode_data(
            "transfer-crc-004", 0, "Final flag test", is_last=False
        )

        message = json.loads(payload)
        message["is_last"] = True

        corrupted_payload = json.dumps(
            message,
            ensure_ascii=False,
            separators=(",", ":")
        )

        decoded, error = decode_data_detailed(corrupted_payload)

        self.assertIsNone(decoded)
        self.assertEqual(error, "checksum_mismatch")


if __name__ == "__main__":
    unittest.main()
