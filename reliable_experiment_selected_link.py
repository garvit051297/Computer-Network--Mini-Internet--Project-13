#!/usr/bin/env python3
"""Track B: best-effort versus reliable Mini Internet transfer experiments.

Quick validation:
    python reliable_experiment.py --quick

Full experiment (4 conditions x 3 repetitions x 2 protocols = 24 trials):
    python reliable_experiment.py

The script does not modify experiment.py or the reliable-transfer implementation.
It temporarily changes link delay/loss in config.json and restores the exact
original config.json bytes in a finally block.
"""

import argparse
import csv
import json
import os
import random
import socket
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

from packet import encode_packet, decode_packet, MAX_PACKET_SIZE
from reliable_protocol import encode_data, decode_data


ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.json"
RESULTS_DIR = ROOT / "results"
LOG_DIR = ROOT / "logs" / "track_b_selected_link"

ROUTERS = ("R_A", "R_B", "R_C", "R_D")
ALL_PORTS = (5001, 5002, 5003, 5004, 5005, 5006)
CONDITIONS = ((5, 0.0), (5, 0.05), (25, 0.0), (25, 0.05))

DEFAULT_REPETITIONS = 3
DEFAULT_MESSAGE_COUNT = 20
QUICK_MESSAGE_COUNT = 5
BEST_EFFORT_OBSERVATION_SECONDS = 2.0
ROUTER_STARTUP_TIMEOUT_SECONDS = 10.0
RELIABLE_RECEIVER_READY_TIMEOUT_SECONDS = 8.0
RELIABLE_RECEIVER_EXIT_TIMEOUT_SECONDS = 20.0
RELIABLE_SENDER_TIMEOUT_SECONDS = 30.0
BASE_SEED = 42000
HOP_LIMIT = 10
RECEIVE_BUFFER_SIZE = MAX_PACKET_SIZE + 1

RAW_FIELDS = [
    "condition_id", "protocol", "repetition", "delay_ms",
    "loss_probability", "router_seed_base", "router_seeds", "message_count",
    "unique_delivered", "delivery_fraction", "duplicate_packets",
    "out_of_order_arrivals", "in_order_delivery",
    "complete_sequence_received", "transfer_complete",
    "sender_success", "retransmissions", "sender_elapsed_seconds",
    "delivery_elapsed_seconds", "best_effort_interval_seconds",
    "observation_window_seconds", "receiver_completed_transfers",
    "receiver_shutdown", "simulated_router_drops", "transfer_id",
    "sender_failure_reason",
]


def read_events(path):
    events = []
    if not path.exists():
        return events
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try:
                item = json.loads(line)
                if isinstance(item, dict) and "event" in item:
                    events.append(item)
            except json.JSONDecodeError:
                continue
    return events


def assert_project_ports_free():
    occupied = []
    opened = []
    try:
        for port in ALL_PORTS:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                sock.bind(("127.0.0.1", port))
                opened.append(sock)
            except OSError:
                occupied.append(port)
                sock.close()
        if occupied:
            raise RuntimeError(
                "These Mini Internet UDP ports are occupied: "
                + ", ".join(map(str, occupied))
                + ". Stop the corresponding project processes first."
            )
    finally:
        for sock in opened:
            sock.close()


def stop_process(process, timeout=3.0):
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=timeout)


def wait_for_event(log_path, event_name, process, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if any(e.get("event") == event_name for e in read_events(log_path)):
            return True
        if process.poll() is not None:
            return False
        time.sleep(0.1)
    return False


def make_trial_config(original_config, delay_ms, loss_probability):
    config = json.loads(json.dumps(original_config))
    selected_links = []
    for link in config["links"]:
        endpoints = {link["from"], link["to"]}
        if endpoints == {"R_B", "R_D"}:
            link["delay_ms"] = delay_ms
            link["loss_prob"] = loss_probability
            selected_links.append(link)

    if len(selected_links) != 1:
        raise RuntimeError(
            f"Expected exactly one R_B-R_D link; found {len(selected_links)}."
        )
    return config


def write_config_atomically(config):
    temp_path = CONFIG_PATH.with_name("config.track_b.tmp")
    with temp_path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(config, handle, indent=2)
        handle.write("\n")
    os.replace(temp_path, CONFIG_PATH)


def start_routers(condition_id, protocol, repetition, seed_base):
    processes = []
    handles = []
    try:
        for index, router_name in enumerate(ROUTERS):
            seed = seed_base + index
            log_path = LOG_DIR / (
                f"{condition_id}_{protocol}_rep{repetition}_{router_name}.log"
            )
            handle = log_path.open("w", encoding="utf-8")
            handles.append(handle)
            process = subprocess.Popen(
                [
                    sys.executable, "-u", str(ROOT / "router.py"),
                    router_name, "--seed", str(seed),
                ],
                cwd=ROOT,
                stdout=handle,
                stderr=subprocess.STDOUT,
            )
            processes.append(process)
            if not wait_for_event(
                log_path, "LISTEN", process, ROUTER_STARTUP_TIMEOUT_SECONDS
            ):
                raise RuntimeError(
                    f"{router_name} failed to start; inspect {log_path}"
                )
        return processes, handles
    except Exception:
        for process in reversed(processes):
            stop_process(process)
        for handle in handles:
            handle.close()
        raise


def stop_routers(processes, handles):
    for process in reversed(processes):
        stop_process(process)
    for handle in handles:
        try:
            handle.close()
        except OSError:
            pass


def count_simulated_drops(condition_id, protocol, repetition):
    total = 0
    for router_name in ROUTERS:
        path = LOG_DIR / (
            f"{condition_id}_{protocol}_rep{repetition}_{router_name}.log"
        )
        total += sum(
            event.get("event") == "DROP"
            and event.get("reason") == "simulated_link_loss"
            for event in read_events(path)
        )
    return total


def build_messages(count):
    return [
        f"Track B message {index:03d}: Mini Internet delivery comparison."
        for index in range(count)
    ]


def run_best_effort(messages, config):
    """Send DATA once at a fixed rate and observe H2 to a fixed deadline."""
    nodes = config["nodes"]
    h1_ip = nodes["H1"]["virtual_ip"]
    h2_ip = nodes["H2"]["virtual_ip"]
    h1_port = nodes["H1"]["udp_port"]
    h2_port = nodes["H2"]["udp_port"]
    gateway_port = nodes[nodes["H1"]["default_gateway"]]["udp_port"]
    transfer_id = uuid.uuid4().hex

    received_sequences = []
    receive_times = []
    receiver_errors = []
    receiver_running = threading.Event()
    receiver_running.set()

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiver:
        receiver.bind(("127.0.0.1", h2_port))
        receiver.settimeout(0.1)

        def receive_loop():
            while receiver_running.is_set():
                try:
                    raw, _endpoint = receiver.recvfrom(RECEIVE_BUFFER_SIZE)
                except socket.timeout:
                    continue
                except OSError as error:
                    receiver_errors.append(str(error))
                    return
                packet = decode_packet(raw)
                if (
                    packet is None
                    or packet["src_ip"] != h1_ip
                    or packet["dst_ip"] != h2_ip
                    or packet["msg_type"] != "DATA"
                ):
                    continue
                data = decode_data(packet["payload"])
                if data is not None and data["transfer_id"] == transfer_id:
                    received_sequences.append(data["seq"])
                    receive_times.append(time.monotonic())

        thread = threading.Thread(target=receive_loop, daemon=True)
        thread.start()

        send_start = time.monotonic()
        submitted = 0
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender:
            sender.bind(("127.0.0.1", h1_port))
            for seq, message in enumerate(messages):
                scheduled = send_start + seq * 0.30
                wait = scheduled - time.monotonic()
                if wait > 0:
                    time.sleep(wait)
                payload = encode_data(
                    transfer_id, seq, message, is_last=(seq == len(messages) - 1)
                )
                packet_bytes = encode_packet(
                    src_ip=h1_ip,
                    dst_ip=h2_ip,
                    hop_limit=HOP_LIMIT,
                    msg_type="DATA",
                    msg_id=random.SystemRandom().randrange(0, 2**32),
                    payload_str=payload,
                )
                sender.sendto(packet_bytes, ("127.0.0.1", gateway_port))
                submitted += 1
        sender_elapsed = time.monotonic() - send_start

        # The observation deadline is fixed relative to the scheduled final send.
        deadline = send_start + (len(messages) - 1) * 0.30 + BEST_EFFORT_OBSERVATION_SECONDS
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
        receiver_running.clear()
        thread.join(timeout=1.0)

    unique = set(received_sequences)
    expected = set(range(len(messages)))
    max_seen = -1
    out_of_order = 0
    for seq in received_sequences:
        if seq < max_seen:
            out_of_order += 1
        max_seen = max(max_seen, seq)
    complete = unique == expected
    delivery_elapsed = ""
    if complete and receive_times:
        # Time from the first scheduled send until the final unique sequence arrives.
        delivery_elapsed = round(max(receive_times) - send_start, 6)

    return {
        "transfer_id": transfer_id,
        "unique_delivered": len(unique),
        "duplicate_packets": len(received_sequences) - len(unique),
        "out_of_order_arrivals": out_of_order,
        "in_order_delivery": received_sequences == list(range(len(messages))),
        "complete_sequence_received": complete,
        "sender_success": submitted == len(messages),
        "transfer_complete": complete,
        "retransmissions": 0,
        "sender_elapsed_seconds": round(sender_elapsed, 6),
        "delivery_elapsed_seconds": delivery_elapsed,
        "best_effort_interval_seconds": 0.30,
        "receiver_completed_transfers": int(complete),
        "receiver_shutdown": True,
        "sender_failure_reason": "" if submitted == len(messages) else "send_incomplete",
        "receiver_errors": receiver_errors,
    }
def run_reliable(messages, condition_id, repetition):
    """Run the project's unchanged reliable H2 receiver and H1 sender."""
    receiver_log = LOG_DIR / (
        f"{condition_id}_reliable_rep{repetition}_H2_receiver.log"
    )
    receiver_handle = receiver_log.open("w", encoding="utf-8")
    receiver = None
    sender_stdout = ""
    sender_stderr = ""
    sender_returncode = None
    receiver_shutdown = False
    sender_wall_elapsed = 0.0

    try:
        receiver = subprocess.Popen(
            [sys.executable, "-u", str(ROOT / "host.py"), "H2", "reliable-receive"],
            cwd=ROOT,
            stdout=receiver_handle,
            stderr=subprocess.STDOUT,
        )
        if not wait_for_event(
            receiver_log,
            "RELIABLE_RECEIVER_READY",
            receiver,
            RELIABLE_RECEIVER_READY_TIMEOUT_SECONDS,
        ):
            raise RuntimeError(
                f"Reliable H2 receiver failed to start; inspect {receiver_log}"
            )

        command = [
            sys.executable, "-u", str(ROOT / "host.py"),
            "H1", "reliable-send", *messages,
        ]
        sender_start = time.monotonic()
        try:
            completed = subprocess.run(
                command,
                cwd=ROOT,
                capture_output=True,
                text=True,
                timeout=RELIABLE_SENDER_TIMEOUT_SECONDS,
                check=False,
            )
            sender_stdout = completed.stdout or ""
            sender_stderr = completed.stderr or ""
            sender_returncode = completed.returncode
        except subprocess.TimeoutExpired as error:
            sender_stdout = error.stdout or ""
            sender_stderr = error.stderr or ""
        sender_wall_elapsed = time.monotonic() - sender_start

        try:
            receiver.wait(timeout=RELIABLE_RECEIVER_EXIT_TIMEOUT_SECONDS)
            receiver_shutdown = True
        except subprocess.TimeoutExpired:
            stop_process(receiver)
    finally:
        if receiver is not None and receiver.poll() is None:
            stop_process(receiver)
        receiver_handle.close()

    sender_events = []
    for line in sender_stdout.splitlines():
        try:
            item = json.loads(line)
            if isinstance(item, dict) and "event" in item:
                sender_events.append(item)
        except json.JSONDecodeError:
            continue

    receiver_events = read_events(receiver_log)
    transfer_start_events = [
        event for event in sender_events if event.get("event") == "TRANSFER_START"
    ]
    transfer_id = (
        transfer_start_events[0].get("transfer_id")
        if transfer_start_events else ""
    )
    if transfer_id:
        receiver_events = [
            event for event in receiver_events
            if event.get("transfer_id") in (None, transfer_id)
        ]

    delivered = [
        event for event in receiver_events if event.get("event") == "DELIVER_DATA"
    ]
    delivery_elapsed = ""
    if transfer_start_events:
        final_delivery = max(
            (event for event in delivered if event.get("seq") == len(messages) - 1),
            key=lambda event: event.get("timestamp", ""),
            default=None,
        )
        if final_delivery and final_delivery.get("timestamp"):
            from datetime import datetime
            start_dt = datetime.fromisoformat(transfer_start_events[0]["timestamp"])
            final_dt = datetime.fromisoformat(final_delivery["timestamp"])
            delivery_elapsed = round((final_dt - start_dt).total_seconds(), 6)
    sequences = [
        event.get("seq") for event in delivered
        if isinstance(event.get("seq"), int)
    ]
    unique = set(sequences)
    expected = set(range(len(messages)))
    duplicates = sum(
        event.get("event") in ("DUPLICATE_DATA", "DUPLICATE_FINAL_DATA")
        for event in receiver_events
    )
    max_seen = -1
    out_of_order = 0
    for seq in sequences:
        if seq < max_seen:
            out_of_order += 1
        max_seen = max(max_seen, seq)

    complete_receiver_events = [
        event for event in receiver_events
        if event.get("event") == "TRANSFER_RECEIVED_COMPLETE"
    ]
    complete_sender_events = [
        event for event in sender_events if event.get("event") == "TRANSFER_COMPLETE"
    ]
    failed_sender_events = [
        event for event in sender_events if event.get("event") == "TRANSFER_FAILED"
    ]
    retransmissions = sum(
        event.get("event") == "SEND_DATA"
        and event.get("retransmission") is True
        for event in sender_events
    )

    if complete_sender_events or failed_sender_events:
        elapsed = (
            complete_sender_events[-1].get("elapsed_seconds")
            if complete_sender_events
            else failed_sender_events[-1].get("elapsed_seconds")
        )
    else:
        elapsed = sender_wall_elapsed
    if not isinstance(elapsed, (int, float)):
        elapsed = sender_wall_elapsed

    if failed_sender_events:
        failure_reason = failed_sender_events[-1].get("reason", "sender_failed")
    elif sender_returncode not in (0, None):
        failure_reason = f"sender_process_exit_{sender_returncode}"
    elif sender_returncode is None:
        failure_reason = "sender_timeout"
    else:
        failure_reason = ""

    sender_success = bool(complete_sender_events) and sender_returncode == 0
    transfer_complete = (
        sender_success
        and bool(complete_receiver_events)
        and unique == expected
    )

    return {
        "transfer_id": transfer_id or "",
        "unique_delivered": len(unique),
        "duplicate_packets": duplicates,
        "out_of_order_arrivals": out_of_order,
        "in_order_delivery": sequences == list(range(len(messages))),
        "complete_sequence_received": unique == expected,
        "sender_success": sender_success,
        "transfer_complete": transfer_complete,
        "retransmissions": retransmissions,
        "sender_elapsed_seconds": round(float(elapsed), 6),
        "delivery_elapsed_seconds": delivery_elapsed,
        "best_effort_interval_seconds": "",
        "receiver_completed_transfers": len(complete_receiver_events),
        "receiver_shutdown": receiver_shutdown,
        "sender_failure_reason": failure_reason,
        "sender_stderr": sender_stderr,
    }


def append_result(path, row):
    new_file = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=RAW_FIELDS)
        if new_file:
            writer.writeheader()
        writer.writerow(row)


def write_summary(raw_path, summary_path):
    rows = []
    with raw_path.open("r", newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    fields = [
        "protocol", "delay_ms", "loss_probability", "repetitions",
        "mean_delivery_fraction", "mean_unique_delivered",
        "complete_transfer_count", "mean_retransmissions",
        "mean_sender_elapsed_seconds", "mean_delivery_elapsed_seconds",
        "successful_delivery_timing_count", "failed_transfer_count",
        "mean_simulated_router_drops",
    ]
    groups = {}
    for row in rows:
        key = (row["protocol"], int(row["delay_ms"]), float(row["loss_probability"]))
        groups.setdefault(key, []).append(row)

    with summary_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for (protocol, delay, loss), group in sorted(groups.items()):
            n = len(group)
            writer.writerow({
                "protocol": protocol,
                "delay_ms": delay,
                "loss_probability": loss,
                "repetitions": n,
                "mean_delivery_fraction": round(
                    sum(float(r["delivery_fraction"]) for r in group) / n, 6
                ),
                "mean_unique_delivered": round(
                    sum(int(r["unique_delivered"]) for r in group) / n, 3
                ),
                "complete_transfer_count": sum(
                    r["transfer_complete"].lower() == "true" for r in group
                ),
                "mean_retransmissions": round(
                    sum(int(r["retransmissions"]) for r in group) / n, 3
                ),
                "mean_sender_elapsed_seconds": round(
                    sum(float(r["sender_elapsed_seconds"]) for r in group) / n, 6
                ),
                "mean_delivery_elapsed_seconds": round(
                    sum(float(r["delivery_elapsed_seconds"]) for r in group
                        if r["delivery_elapsed_seconds"] not in ("", None)) /
                    max(1, sum(r["delivery_elapsed_seconds"] not in ("", None)
                               for r in group)), 6
                ) if any(r["delivery_elapsed_seconds"] not in ("", None)
                         for r in group) else "",
                "successful_delivery_timing_count": sum(
                    r["delivery_elapsed_seconds"] not in ("", None)
                    for r in group
                ),
                "failed_transfer_count": sum(
                    r["transfer_complete"].lower() != "true"
                    for r in group
                ),
                "mean_simulated_router_drops": round(
                    sum(int(r["simulated_router_drops"]) for r in group) / n, 3
                ),
            })
def run_trial(protocol, condition_index, delay_ms, loss_probability,
              repetition, message_count, original_config, raw_path):
    condition_id = f"d{delay_ms}_l{int(loss_probability * 100):02d}"
    seed_base = BASE_SEED + condition_index * 1000 + repetition * 10
    trial_config = make_trial_config(original_config, delay_ms, loss_probability)
    write_config_atomically(trial_config)
    assert_project_ports_free()

    processes = []
    handles = []
    try:
        processes, handles = start_routers(
            condition_id, protocol, repetition, seed_base
        )
        messages = build_messages(message_count)

        if protocol == "best_effort":
            result = run_best_effort(messages, trial_config)
        else:
            result = run_reliable(messages, condition_id, repetition)

        drops = count_simulated_drops(condition_id, protocol, repetition)
        row = {
            "condition_id": condition_id,
            "protocol": protocol,
            "repetition": repetition,
            "delay_ms": delay_ms,
            "loss_probability": loss_probability,
            "router_seed_base": seed_base,
            "router_seeds": ";".join(
                f"{name}:{seed_base + index}"
                for index, name in enumerate(ROUTERS)
            ),
            "message_count": message_count,
            "unique_delivered": result["unique_delivered"],
            "delivery_fraction": round(result["unique_delivered"] / message_count, 6),
            "duplicate_packets": result["duplicate_packets"],
            "out_of_order_arrivals": result["out_of_order_arrivals"],
            "in_order_delivery": result["in_order_delivery"],
            "complete_sequence_received": result["complete_sequence_received"],
            "transfer_complete": result["transfer_complete"],
            "sender_success": result["sender_success"],
            "retransmissions": result["retransmissions"],
            "sender_elapsed_seconds": result["sender_elapsed_seconds"],
            "delivery_elapsed_seconds": result.get("delivery_elapsed_seconds", ""),
            "best_effort_interval_seconds": result.get("best_effort_interval_seconds", ""),
            "observation_window_seconds": (
                BEST_EFFORT_OBSERVATION_SECONDS if protocol == "best_effort" else ""
            ),
            "receiver_completed_transfers": result["receiver_completed_transfers"],
            "receiver_shutdown": result["receiver_shutdown"],
            "simulated_router_drops": drops,
            "transfer_id": result["transfer_id"],
            "sender_failure_reason": result["sender_failure_reason"],
        }
        append_result(raw_path, row)
        print(
            f"  {protocol}: {row['unique_delivered']}/{message_count} delivered; "
            f"complete={row['transfer_complete']}; "
            f"retransmissions={row['retransmissions']}; "
            f"sender_elapsed={row['sender_elapsed_seconds']} s; "
            f"simulated_drops={drops}",
            flush=True,
        )
        if result.get("receiver_errors"):
            print(f"  Receiver errors: {result['receiver_errors']}", flush=True)
        if result.get("sender_stderr"):
            print(f"  Sender stderr: {result['sender_stderr']}", flush=True)
        return row
    finally:
        stop_routers(processes, handles)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compare best-effort and reliable Mini Internet transfers."
    )
    parser.add_argument(
        "--quick", action="store_true",
        help="Run one 5 ms/0%% condition, one repetition, five messages (2 trials).",
    )
    parser.add_argument(
        "--repetitions", type=int, default=DEFAULT_REPETITIONS,
        help="Repetitions per condition and protocol (default: 3).",
    )
    parser.add_argument(
        "--messages", type=int, default=DEFAULT_MESSAGE_COUNT,
        help="Messages per transfer (default: 20).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if args.repetitions < 1 or args.messages < 1:
        raise SystemExit("--repetitions and --messages must both be positive.")

    RESULTS_DIR.mkdir(exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    suffix = "quick" if args.quick else "full"
    raw_path = RESULTS_DIR / f"track_b_selected_link_{suffix}_raw.csv"
    summary_path = RESULTS_DIR / f"track_b_selected_link_{suffix}_summary.csv"

    if raw_path.exists() or summary_path.exists():
        raise SystemExit(
            "Track B result file already exists; refusing to overwrite it:\n"
            f"  {raw_path}\n  {summary_path}\n"
            "Move/rename the previous Track B results before starting a new run."
        )

    original_bytes = CONFIG_PATH.read_bytes()
    original_config = json.loads(original_bytes.decode("utf-8-sig"))
    # Preserve pre-existing router logs; the router logger itself opens these
    # standard filenames in write mode during each trial.
    router_log_backups = {}
    for router_name in ROUTERS:
        router_log = ROOT / "logs" / f"{router_name}.log"
        router_log_backups[router_log] = (
            router_log.read_bytes() if router_log.exists() else None
        )

    if args.quick:
        conditions = (CONDITIONS[0],)
        repetitions = 1
        message_count = QUICK_MESSAGE_COUNT
    else:
        conditions = CONDITIONS
        repetitions = args.repetitions
        message_count = args.messages

    total = len(conditions) * repetitions * 2
    completed = 0
    print("TRACK B: BEST-EFFORT vs RELIABLE TRANSFER", flush=True)
    print(f"Conditions: {conditions}", flush=True)
    print(f"Repetitions per condition/protocol: {repetitions}", flush=True)
    print(f"Messages per transfer: {message_count}", flush=True)
    print(f"Total trials: {total}", flush=True)
    print(
        "Reliable sender settings are unchanged: 0.5 s ACK timeout, "
        "5 maximum retransmissions; receiver grace period 15 s.",
        flush=True,
    )
    print(
        f"Best-effort receiver observation window: "
        f"{BEST_EFFORT_OBSERVATION_SECONDS:.1f} s after the final scheduled send; "
        f"messages are spaced 0.30 s apart.",
        flush=True,
    )

    try:
        assert_project_ports_free()
        for condition_index, (delay_ms, loss_probability) in enumerate(conditions):
            condition_id = f"d{delay_ms}_l{int(loss_probability * 100):02d}"
            print(
                f"\nCondition {condition_index + 1}/{len(conditions)}: "
                f"{delay_ms} ms delay, {loss_probability:.0%} loss",
                flush=True,
            )
            for repetition in range(1, repetitions + 1):
                print(f" Repetition {repetition}/{repetitions}", flush=True)
                # Alternate protocol order to reduce systematic order effects.
                order = (
                    ("best_effort", "reliable")
                    if (condition_index + repetition) % 2
                    else ("reliable", "best_effort")
                )
                for protocol in order:
                    run_trial(
                        protocol, condition_index, delay_ms, loss_probability,
                        repetition, message_count, original_config, raw_path,
                    )
                    completed += 1
                    print(f"  Progress: {completed}/{total}", flush=True)

        write_summary(raw_path, summary_path)
        print("\nTRACK B EXPERIMENT COMPLETED.", flush=True)
        print(f"Raw results: {raw_path}", flush=True)
        print(f"Summary: {summary_path}", flush=True)
    finally:
        # Restore exact original bytes, not merely equivalent JSON values.
        CONFIG_PATH.write_bytes(original_bytes)
        for router_log, contents in router_log_backups.items():
            if contents is None:
                if router_log.exists():
                    router_log.unlink()
            else:
                router_log.write_bytes(contents)
        print("\nOriginal config.json and pre-existing router logs restored.", flush=True)

    if completed != total:
        raise SystemExit(f"Stopped early: {completed}/{total} trials completed.")


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(f"EXPERIMENT ERROR: {error}", file=sys.stderr, flush=True)
        sys.exit(1)