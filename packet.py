import struct

# Packet format:
# 16s = 16 bytes for source virtual IP
# 16s = 16 bytes for destination virtual IP
# B   = 1 byte for hop limit
# 8s  = 8 bytes for message type
# I   = 4 bytes for message ID
HEADER_FORMAT = "!16s16sB8sI"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

MAX_PACKET_SIZE = 1200
MAX_PAYLOAD_SIZE = MAX_PACKET_SIZE - HEADER_SIZE


def encode_packet(src_ip, dst_ip, hop_limit, msg_type, msg_id, payload_str):
    """Convert packet fields into bytes for UDP transmission."""

    if not isinstance(src_ip, str) or not isinstance(dst_ip, str):
        raise ValueError("Source and destination IPs must be strings.")

    if not isinstance(msg_type, str) or not msg_type:
        raise ValueError("Message type must be a non-empty string.")

    if not isinstance(payload_str, str):
        raise ValueError("Payload must be a string.")

    src_b = src_ip.encode("utf-8")
    dst_b = dst_ip.encode("utf-8")
    type_b = msg_type.encode("utf-8")
    payload_b = payload_str.encode("utf-8")

    if not src_b or len(src_b) > 16:
        raise ValueError("Source IP must occupy 1 to 16 bytes.")

    if not dst_b or len(dst_b) > 16:
        raise ValueError("Destination IP must occupy 1 to 16 bytes.")

    if len(type_b) > 8:
        raise ValueError("Message type must occupy at most 8 bytes.")

    if not isinstance(hop_limit, int) or isinstance(hop_limit, bool):
        raise ValueError("Hop limit must be an integer.")

    if not 0 <= hop_limit <= 255:
        raise ValueError("Hop limit must be between 0 and 255.")

    if not isinstance(msg_id, int) or isinstance(msg_id, bool):
        raise ValueError("Message ID must be an integer.")

    if not 0 <= msg_id <= 4294967295:
        raise ValueError("Message ID must be between 0 and 4294967295.")

    if len(payload_b) > MAX_PAYLOAD_SIZE:
        raise ValueError(
            f"Payload is too large. Maximum: {MAX_PAYLOAD_SIZE} bytes."
        )

    # Fixed-width fields are padded with zero bytes.
    header = struct.pack(
        HEADER_FORMAT,
        src_b,
        dst_b,
        hop_limit,
        type_b,
        msg_id
    )

    return header + payload_b


def decode_packet(data):
    """Convert received packet bytes back into readable fields."""

    if not isinstance(data, bytes):
        return None

    if len(data) < HEADER_SIZE or len(data) > MAX_PACKET_SIZE:
        return None

    header = data[:HEADER_SIZE]
    payload_b = data[HEADER_SIZE:]

    try:
        src_b, dst_b, hop_limit, type_b, msg_id = struct.unpack(
            HEADER_FORMAT, header
        )

        return {
            "src_ip": src_b.rstrip(b"\x00").decode("utf-8"),
            "dst_ip": dst_b.rstrip(b"\x00").decode("utf-8"),
            "hop_limit": hop_limit,
            "msg_type": type_b.rstrip(b"\x00").decode("utf-8"),
            "msg_id": msg_id,
            "payload": payload_b.decode("utf-8")
        }

    except (struct.error, UnicodeDecodeError):
        return None