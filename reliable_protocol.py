
"""Payload helpers for the Mini Internet reliable-transfer protocol."""

import json
import zlib
from typing import Any

MAX_PAYLOAD_SIZE = 1155


def _require_transfer_id(transfer_id: Any) -> str:
    """Validate a transfer identifier."""
    if not isinstance(transfer_id, str):
        raise ValueError("transfer_id must be a string")

    if not transfer_id or len(transfer_id) > 128:
        raise ValueError("transfer_id must contain 1 to 128 characters")

    return transfer_id


def _require_sequence(seq: Any) -> int:
    """Validate a zero-based sequence number."""
    if isinstance(seq, bool) or not isinstance(seq, int):
        raise ValueError("seq must be an integer")

    if seq < 0 or seq > 2**32 - 1:
        raise ValueError("seq must be between 0 and 4294967295")

    return seq


def _check_payload_size(payload: str) -> str:
    """Reject payloads exceeding the packet module's byte limit."""
    payload_size = len(payload.encode("utf-8"))

    if payload_size > MAX_PAYLOAD_SIZE:
        raise ValueError(
            f"Payload is too large: {payload_size} bytes; "
            f"maximum is {MAX_PAYLOAD_SIZE} bytes."
        )

    return payload


def calculate_crc32(data: str) -> str:
    """Calculate the CRC-32 checksum of UTF-8 application data."""
    if not isinstance(data, str):
        raise ValueError("data must be a string")

    checksum = zlib.crc32(data.encode("utf-8")) & 0xFFFFFFFF
    return f"{checksum:08x}"


def _calculate_data_crc32(
    transfer_id: str,
    seq: int,
    data: str,
    is_last: bool,
) -> str:
    """Calculate CRC-32 over all DATA message fields."""
    checksum_source = json.dumps(
        {
            "transfer_id": transfer_id,
            "seq": seq,
            "data": data,
            "is_last": is_last,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )

    checksum = zlib.crc32(
        checksum_source.encode("utf-8")
    ) & 0xFFFFFFFF

    return f"{checksum:08x}"


def encode_data(
    transfer_id: str,
    seq: int,
    data: str,
    is_last: bool = False,
) -> str:
    """Encode DATA with a CRC-32 integrity checksum."""
    transfer_id = _require_transfer_id(transfer_id)
    seq = _require_sequence(seq)

    if not isinstance(data, str):
        raise ValueError("data must be a string")

    if not isinstance(is_last, bool):
        raise ValueError("is_last must be a Boolean")

    payload = json.dumps(
        {
            "transfer_id": transfer_id,
            "seq": seq,
            "data": data,
            "is_last": is_last,
            "crc32": _calculate_data_crc32(
                transfer_id, seq, data, is_last
            ),
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )

    return _check_payload_size(payload)


def decode_data_detailed(
    payload: str,
) -> tuple[dict[str, Any] | None, str | None]:
    """Decode DATA and distinguish checksum failures from invalid payloads."""
    if not isinstance(payload, str):
        return None, "invalid_data_payload"

    try:
        _check_payload_size(payload)
        message = json.loads(payload)

        if not isinstance(message, dict):
            return None, "invalid_data_payload"

        if set(message) != {
            "transfer_id", "seq", "data", "is_last", "crc32"
        }:
            return None, "invalid_data_payload"

        transfer_id = _require_transfer_id(message["transfer_id"])
        seq = _require_sequence(message["seq"])
        data = message["data"]
        is_last = message["is_last"]
        supplied_checksum = message["crc32"]

        if not isinstance(data, str):
            return None, "invalid_data_payload"

        if not isinstance(is_last, bool):
            return None, "invalid_data_payload"

        if (
            not isinstance(supplied_checksum, str)
            or len(supplied_checksum) != 8
        ):
            return None, "invalid_data_payload"

        try:
            int(supplied_checksum, 16)
        except ValueError:
            return None, "invalid_data_payload"

        expected_checksum = _calculate_data_crc32(
            transfer_id, seq, data, is_last
        )

        if supplied_checksum.lower() != expected_checksum:
            return None, "checksum_mismatch"

        return {
            "transfer_id": transfer_id,
            "seq": seq,
            "data": data,
            "is_last": is_last,
        }, None

    except (ValueError, TypeError, json.JSONDecodeError):
        return None, "invalid_data_payload"


def decode_data(payload: str) -> dict[str, Any] | None:
    """Decode and verify DATA; return None if invalid or corrupted."""
    message, _error = decode_data_detailed(payload)
    return message


def encode_ack(transfer_id: str, seq: int) -> str:
    """Encode a size-checked ACK payload as JSON."""
    transfer_id = _require_transfer_id(transfer_id)
    seq = _require_sequence(seq)

    payload = json.dumps(
        {
            "transfer_id": transfer_id,
            "seq": seq,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )

    return _check_payload_size(payload)


def decode_ack(payload: str) -> dict[str, Any] | None:
    """Decode ACK; return None if its format or values are invalid."""
    if not isinstance(payload, str):
        return None

    try:
        _check_payload_size(payload)
        message = json.loads(payload)

        if not isinstance(message, dict):
            return None

        if set(message) != {"transfer_id", "seq"}:
            return None

        transfer_id = _require_transfer_id(message["transfer_id"])
        seq = _require_sequence(message["seq"])

        return {
            "transfer_id": transfer_id,
            "seq": seq,
        }

    except (ValueError, TypeError, json.JSONDecodeError):
        return None
