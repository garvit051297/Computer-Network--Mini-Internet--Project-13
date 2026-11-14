
"""Stop-and-wait reliable transfer for the Mini Internet project."""

import json
import socket
import time
import uuid
from datetime import datetime, timezone

from packet import encode_packet, decode_packet, MAX_PACKET_SIZE
from reliable_protocol import (
    encode_data,
    decode_data_detailed,
    encode_ack,
    decode_ack,
)


RECEIVE_BUFFER_SIZE = MAX_PACKET_SIZE + 1
DEFAULT_HOP_LIMIT = 10
DEFAULT_ACK_TIMEOUT = 0.5
DEFAULT_MAX_RETRIES = 5
DEFAULT_RECEIVER_GRACE_SECONDS = 15.0


def log_event(device, event, **details):
    """Print one timestamped JSON event."""
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device": device,
        "event": event,
    }
    record.update(details)
    print(json.dumps(record, ensure_ascii=False), flush=True)


def _make_packet(src_ip, dst_ip, msg_type, msg_id, payload):
    """Build a packet using the project's existing packet format."""
    return encode_packet(
        src_ip=src_ip,
        dst_ip=dst_ip,
        hop_limit=DEFAULT_HOP_LIMIT,
        msg_type=msg_type,
        msg_id=msg_id,
        payload_str=payload,
    )


def run_reliable_sender(
    sock,
    host_ip,
    gateway_name,
    gateway_port,
    destination_ip,
    messages,
    ack_timeout=DEFAULT_ACK_TIMEOUT,
    max_retries=DEFAULT_MAX_RETRIES,
    transfer_id=None,
):
    """Send finite messages using stop-and-wait ARQ.

    max_retries counts retransmissions after the initial transmission.
    The same packet and message ID are reused for each retry.
    """
    if (
        isinstance(ack_timeout, bool)
        or not isinstance(ack_timeout, (int, float))
        or ack_timeout <= 0
    ):
        raise ValueError("ack_timeout must be positive")

    if (
        isinstance(max_retries, bool)
        or not isinstance(max_retries, int)
        or max_retries < 0
    ):
        raise ValueError("max_retries must be a non-negative integer")

    if not isinstance(messages, (list, tuple)) or not messages:
        raise ValueError("messages must be a non-empty list or tuple")

    if transfer_id is None:
        transfer_id = uuid.uuid4().hex

    # Validate every payload before transmitting any part of the transfer.
    data_payloads = []
    for seq, message in enumerate(messages):
        payload = encode_data(
            transfer_id,
            seq,
            message,
            is_last=(seq == len(messages) - 1),
        )
        data_payloads.append(payload)

    start_time = time.monotonic()
    total_retransmissions = 0
    destination_endpoint = ("127.0.0.1", gateway_port)

    log_event(
        "H1",
        "TRANSFER_START",
        transfer_id=transfer_id,
        destination_ip=destination_ip,
        message_count=len(messages),
        ack_timeout_seconds=ack_timeout,
        max_retries=max_retries,
    )

    for seq, payload in enumerate(data_payloads):
        # One stable ID per DATA message, reused on retransmission.
        msg_id = uuid.uuid4().int & 0xFFFFFFFF
        packet_bytes = _make_packet(
            host_ip, destination_ip, "DATA", msg_id, payload
        )
        acknowledged = False

        for attempt in range(max_retries + 1):
            retransmission = attempt > 0
            if retransmission:
                total_retransmissions += 1

            log_event(
                "H1",
                "SEND_DATA",
                transfer_id=transfer_id,
                seq=seq,
                msg_id=msg_id,
                attempt=attempt + 1,
                retransmission=retransmission,
                is_last=(seq == len(data_payloads) - 1),
            )

            sock.sendto(packet_bytes, destination_endpoint)
            deadline = time.monotonic() + ack_timeout

            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break

                sock.settimeout(remaining)

                try:
                    received, source_endpoint = sock.recvfrom(
                        RECEIVE_BUFFER_SIZE
                    )
                except socket.timeout:
                    break

                response = decode_packet(received)
                if response is None:
                    log_event(
                        "H1", "IGNORE_PACKET", reason="malformed_packet"
                    )
                    continue

                # All four fields must identify the outstanding DATA.
                if (
                    response["msg_type"] != "ACK"
                    or response["src_ip"] != destination_ip
                    or response["dst_ip"] != host_ip
                    or response["msg_id"] != msg_id
                ):
                    log_event(
                        "H1",
                        "IGNORE_PACKET",
                        reason="packet_does_not_match_outstanding_data",
                        received_type=response["msg_type"],
                        received_msg_id=response["msg_id"],
                    )
                    continue

                ack = decode_ack(response["payload"])
                if (
                    ack is None
                    or ack["transfer_id"] != transfer_id
                    or ack["seq"] != seq
                ):
                    log_event(
                        "H1",
                        "IGNORE_ACK",
                        transfer_id=transfer_id,
                        expected_seq=seq,
                    )
                    continue

                log_event(
                    "H1",
                    "ACK_RECEIVED",
                    transfer_id=transfer_id,
                    seq=seq,
                    msg_id=msg_id,
                    udp_source=(
                        f"{source_endpoint[0]}:{source_endpoint[1]}"
                    ),
                )
                acknowledged = True
                break

            if acknowledged:
                break

            log_event(
                "H1",
                "ACK_TIMEOUT",
                transfer_id=transfer_id,
                seq=seq,
                msg_id=msg_id,
                attempt=attempt + 1,
            )

        if not acknowledged:
            elapsed = time.monotonic() - start_time
            log_event(
                "H1",
                "TRANSFER_FAILED",
                transfer_id=transfer_id,
                failed_seq=seq,
                retransmissions=total_retransmissions,
                elapsed_seconds=round(elapsed, 6),
                reason="retry_limit_exhausted",
            )
            return {
                "success": False,
                "transfer_id": transfer_id,
                "failed_seq": seq,
                "retransmissions": total_retransmissions,
                "elapsed_seconds": elapsed,
            }

    elapsed = time.monotonic() - start_time
    log_event(
        "H1",
        "TRANSFER_COMPLETE",
        transfer_id=transfer_id,
        message_count=len(messages),
        retransmissions=total_retransmissions,
        elapsed_seconds=round(elapsed, 6),
    )
    return {
        "success": True,
        "transfer_id": transfer_id,
        "message_count": len(messages),
        "retransmissions": total_retransmissions,
        "elapsed_seconds": elapsed,
    }


def run_reliable_receiver(
    sock,
    host_ip,
    gateway_name,
    gateway_ip,
    gateway_port,
    expected_sender_ip,
    grace_seconds=DEFAULT_RECEIVER_GRACE_SECONDS,
):
    """Receive one or more transfers, delivering each sequence only once.

    The receiver exits after a fixed grace interval with no accepted DATA.
    Duplicate packets do not extend that interval, so its lifetime is bounded.
    """
    if (
        isinstance(grace_seconds, bool)
        or not isinstance(grace_seconds, (int, float))
        or grace_seconds <= 0
    ):
        raise ValueError("grace_seconds must be positive")

    expected_seq_by_transfer = {}
    completed_transfers = set()
    final_seq_by_transfer = {}
    last_activity = time.monotonic()

    log_event(
        "H2",
        "RELIABLE_RECEIVER_READY",
        virtual_ip=host_ip,
        expected_sender_ip=expected_sender_ip,
        grace_seconds=grace_seconds,
    )

    while True:
        remaining = grace_seconds - (time.monotonic() - last_activity)
        if remaining <= 0:
            log_event(
                "H2",
                "RECEIVER_SHUTDOWN",
                reason="grace_period_expired",
                completed_transfer_count=len(completed_transfers),
            )
            return

        sock.settimeout(remaining)
        try:
            raw_packet, source_endpoint = sock.recvfrom(
                RECEIVE_BUFFER_SIZE
            )
        except socket.timeout:
            log_event(
                "H2",
                "RECEIVER_SHUTDOWN",
                reason="grace_period_expired",
                completed_transfer_count=len(completed_transfers),
            )
            return

        packet = decode_packet(raw_packet)
        if packet is None:
            log_event("H2", "DROP", reason="malformed_or_oversized_packet")
            continue

        if (
            packet["dst_ip"] != host_ip
            or packet["src_ip"] != expected_sender_ip
            or packet["msg_type"] != "DATA"
        ):
            log_event(
                "H2",
                "IGNORE_PACKET",
                reason="not_expected_data_packet",
                msg_type=packet["msg_type"],
                src_ip=packet["src_ip"],
                dst_ip=packet["dst_ip"],
            )
            continue

        data_message, data_error = decode_data_detailed(
            packet["payload"]
        )

        if data_message is None:
            log_event(
                "H2",
                "DROP",
                reason=data_error or "invalid_data_payload",
                msg_id=packet["msg_id"],
            )
            continue

        transfer_id = data_message["transfer_id"]
        seq = data_message["seq"]
        is_last = data_message["is_last"]
        expected_seq = expected_seq_by_transfer.get(transfer_id, 0)

        # Completed transfers remain recognizable during the grace period.
        if transfer_id in completed_transfers:
            is_duplicate_final = (
                is_last and seq == final_seq_by_transfer[transfer_id]
            )
            if is_duplicate_final:
                log_event(
                    "H2",
                    "DUPLICATE_FINAL_DATA",
                    transfer_id=transfer_id,
                    seq=seq,
                    msg_id=packet["msg_id"],
                )
                _send_ack(
                    sock,
                    host_ip,
                    gateway_name,
                    gateway_port,
                    expected_sender_ip,
                    transfer_id,
                    seq,
                    packet["msg_id"],
                )
            else:
                log_event(
                    "H2",
                    "IGNORE_DATA",
                    reason="transfer_already_completed",
                    transfer_id=transfer_id,
                    seq=seq,
                )
            # Deliberately do not reset last_activity here.
            continue

        if seq < expected_seq:
            # A duplicate of an already delivered message: ACK it again.
            log_event(
                "H2",
                "DUPLICATE_DATA",
                transfer_id=transfer_id,
                seq=seq,
                expected_seq=expected_seq,
                msg_id=packet["msg_id"],
            )
            _send_ack(
                sock,
                host_ip,
                gateway_name,
                gateway_port,
                expected_sender_ip,
                transfer_id,
                seq,
                packet["msg_id"],
            )
            continue

        if seq > expected_seq:
            # Do not fabricate an ACK for an unexpected sequence.
            log_event(
                "H2",
                "OUT_OF_ORDER_DATA",
                transfer_id=transfer_id,
                seq=seq,
                expected_seq=expected_seq,
                msg_id=packet["msg_id"],
            )
            continue

        # Deliver the next expected sequence exactly once.
        log_event(
            "H2",
            "DELIVER_DATA",
            transfer_id=transfer_id,
            seq=seq,
            msg_id=packet["msg_id"],
            data=data_message["data"],
            is_last=is_last,
        )
        expected_seq_by_transfer[transfer_id] = expected_seq + 1

        _send_ack(
            sock,
            host_ip,
            gateway_name,
            gateway_port,
            expected_sender_ip,
            transfer_id,
            seq,
            packet["msg_id"],
        )
        last_activity = time.monotonic()

        if is_last:
            completed_transfers.add(transfer_id)
            final_seq_by_transfer[transfer_id] = seq
            log_event(
                "H2",
                "TRANSFER_RECEIVED_COMPLETE",
                transfer_id=transfer_id,
                final_seq=seq,
                grace_seconds=grace_seconds,
            )


def _send_ack(
    sock,
    host_ip,
    gateway_name,
    gateway_port,
    destination_ip,
    transfer_id,
    seq,
    msg_id,
):
    """Send an ACK through the receiver's configured default gateway."""
    try:
        payload = encode_ack(transfer_id, seq)
        ack_packet = _make_packet(
            host_ip, destination_ip, "ACK", msg_id, payload
        )
    except (ValueError, TypeError) as error:
        log_event(
            "H2",
            "ACK_ENCODING_ERROR",
            transfer_id=transfer_id,
            seq=seq,
            error=str(error),
        )
        return

    sock.sendto(ack_packet, ("127.0.0.1", gateway_port))
    log_event(
        "H2",
        "SEND_ACK",
        transfer_id=transfer_id,
        seq=seq,
        msg_id=msg_id,
        next_hop=gateway_name,
        udp_destination=f"127.0.0.1:{gateway_port}",
    )
