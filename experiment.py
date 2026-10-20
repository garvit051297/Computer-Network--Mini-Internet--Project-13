import argparse
import csv
import json
import random
import select
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from packet import encode_packet, decode_packet, MAX_PACKET_SIZE


# ------------------------------------------------------------
# Project paths and experiment settings
# ------------------------------------------------------------

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.json"
ROUTER_PATH = ROOT / "router.py"
RESULTS_DIR = ROOT / "results"
LOGS_DIR = ROOT / "logs"

HOST_IP = "10.0.0.1"
DEST_IP = "10.0.0.6"

HOST_PORT = 5001
GATEWAY_PORT = 5002
H2_PORT = 5006

HOP_LIMIT = 10
PAYLOAD = "Mini Internet baseline experiment"

TIMEOUT_SECONDS = 8.0
INTER_PROBE_INTERVAL_SECONDS = 0.3
LATE_RESPONSE_GRACE_SECONDS = 0.5

PROBES_PER_REPETITION = 20
REPETITIONS = 3

ROUTERS = ["R_A", "R_B", "R_C", "R_D"]

ROUTER_SEED_OFFSETS = {
    "R_A": 101,
    "R_B": 202,
    "R_C": 303,
    "R_D": 404,
}

CONDITIONS = [
    {"delay_ms": 5, "loss_prob": 0.00},
    {"delay_ms": 5, "loss_prob": 0.05},
    {"delay_ms": 25, "loss_prob": 0.00},
    {"delay_ms": 25, "loss_prob": 0.05},
]

RAW_FIELDS = [
    "run_id",
    "delay_ms_per_link",
    "loss_probability_per_link",
    "repetition",
    "base_seed",
    "router_seeds",
    "probe_index",
    "msg_id",
    "send_timestamp_utc",
    "receive_timestamp_utc",
    "outcome",
    "rtt_ms",
    "late_matching_echoes",
    "ignored_responses",
    "timeout_seconds",
    "late_response_grace_seconds",
    "payload",
]

SUMMARY_FIELDS = [
    "delay_ms_per_link",
    "loss_probability_per_link",
    "repetition",
    "probes_sent",
    "successes",
    "timeouts",
    "late_responses",
    "delivery_fraction",
    "mean_successful_rtt_ms",
    "min_successful_rtt_ms",
    "max_successful_rtt_ms",
    "successful_rtt_range_ms",
]


# ------------------------------------------------------------
# File and configuration helpers
# ------------------------------------------------------------

def utc_now():
    """Return the current UTC time as an ISO-formatted string."""
    return datetime.now(timezone.utc).isoformat()


def save_json(path, data):
    """Save JSON using consistent indentation and a trailing newline."""
    with path.open("w", encoding="utf-8", newline="\n") as file:
        json.dump(data, file, indent=2)
        file.write("\n")


def write_csv(path, rows, fields):
    """Write CSV rows and their header."""
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def configure_condition(baseline, delay_ms, loss_prob):
    """Change link delay and loss without changing routes or topology."""
    config = json.loads(json.dumps(baseline))

    for link in config["links"]:
        link["delay_ms"] = delay_ms
        link["loss_prob"] = loss_prob

    return config


def refuse_to_overwrite_results(raw_path, summary_path):
    """
    Protect existing result files from accidental replacement.

    Rename or archive old result files before starting a fresh experiment.
    """
    for path in (raw_path, summary_path):
        if path.exists() and path.stat().st_size > 0:
            raise RuntimeError(
                f"Results file already exists and is nonempty: {path}\n"
                "The script will not overwrite previous data. "
                "Archive or rename the existing file before rerunning."
            )


# ------------------------------------------------------------
# Process and socket safety checks
# ------------------------------------------------------------

def verify_h2_is_running():
    """Check that H2's UDP port is occupied."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as test_sock:
        try:
            test_sock.bind(("127.0.0.1", H2_PORT))
        except OSError:
            return

    raise RuntimeError(
        "UDP port 5006 is not occupied. Confirm that H2 is running "
        "in another PowerShell window using: python .\\host.py H2"
    )


def ensure_router_ports_free(config):
    """Refuse to launch routers if their UDP ports are already occupied."""
    for router in ROUTERS:
        port = config["nodes"][router]["udp_port"]

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as test_sock:
            try:
                test_sock.bind(("127.0.0.1", port))
            except OSError as error:
                raise RuntimeError(
                    f"Router UDP port {port} for {router} is occupied. "
                    "Identify the process before continuing."
                ) from error


def wait_for_routers(processes, expected_seeds, log_paths, timeout=10.0):
    """Wait for fresh LISTEN records and verify the processes remain alive."""
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        for router, process in processes.items():
            if process.poll() is not None:
                raise RuntimeError(
                    f"{router} exited during startup. "
                    f"Inspect {log_paths[router]}"
                )

        all_ready = True

        for router in ROUTERS:
            try:
                lines = log_paths[router].read_text(
                    encoding="utf-8"
                ).splitlines()
            except OSError:
                all_ready = False
                break

            found = False

            for line in lines:
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if (
                    event.get("device") == router
                    and event.get("event") == "LISTEN"
                    and event.get("seed") == expected_seeds[router]
                ):
                    found = True
                    break

            if not found:
                all_ready = False
                break

        if all_ready:
            time.sleep(0.25)

            for router, process in processes.items():
                if process.poll() is not None:
                    raise RuntimeError(
                        f"{router} exited immediately after startup. "
                        f"Inspect {log_paths[router]}"
                    )

            return

        time.sleep(0.1)

    raise RuntimeError(
        "Timed out waiting for all seeded routers to report LISTEN. "
        "Inspect logs/experiment_*.stdout.log."
    )


def stop_routers(processes):
    """Stop only router processes launched and tracked by this script."""
    for process in processes.values():
        if process.poll() is None:
            process.terminate()

    for process in processes.values():
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def start_routers(base_seed, config):
    """Launch four routers and record each router's seed."""
    ensure_router_ports_free(config)

    processes = {}
    handles = {}
    log_paths = {}

    seeds = {
        router: base_seed + ROUTER_SEED_OFFSETS[router]
        for router in ROUTERS
    }

    try:
        for router in ROUTERS:
            log_path = LOGS_DIR / f"experiment_{router}.stdout.log"
            log_paths[router] = log_path

            handle = log_path.open("w", encoding="utf-8")
            handles[router] = handle

            process = subprocess.Popen(
                [
                    sys.executable,
                    str(ROUTER_PATH),
                    router,
                    "--seed",
                    str(seeds[router]),
                ],
                cwd=ROOT,
                stdout=handle,
                stderr=subprocess.STDOUT,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )

            processes[router] = process

        wait_for_routers(processes, seeds, log_paths)
        return processes, handles, seeds

    except Exception:
        stop_routers(processes)

        for handle in handles.values():
            handle.close()

        raise


# ------------------------------------------------------------
# Fixed-interval probe measurement
# ------------------------------------------------------------

def run_probes(condition, repetition, base_seed, router_seeds):
    """
    Send probes on a fixed schedule and measure matching echo RTT.

    Matching echoes must have the expected virtual addresses, packet type,
    message ID, and payload. The UDP response must come from R_A's gateway
    socket on port GATEWAY_PORT, because R_A forwards the echo to H1.
    """
    run_id = (
        f"d{condition['delay_ms']}_"
        f"l{int(condition['loss_prob'] * 100):02d}_"
        f"rep{repetition}"
    )

    probes = {}
    ordered_ids = []
    ignored_unmatched = 0

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", HOST_PORT))
        sock.setblocking(False)

        start_time = time.monotonic()
        next_send_time = start_time
        next_probe_index = 1

        while True:
            now = time.monotonic()

            # Send probes on a fixed schedule, independently of responses.
            while (
                next_probe_index <= PROBES_PER_REPETITION
                and now >= next_send_time
            ):
                probe_index = next_probe_index
                msg_id = random.SystemRandom().randrange(0, 2**32)

                while msg_id in probes:
                    msg_id = random.SystemRandom().randrange(0, 2**32)

                packet = encode_packet(
                    src_ip=HOST_IP,
                    dst_ip=DEST_IP,
                    hop_limit=HOP_LIMIT,
                    msg_type="PROBE",
                    msg_id=msg_id,
                    payload_str=PAYLOAD,
                )

                send_monotonic = time.monotonic()
                send_timestamp = utc_now()

                sock.sendto(packet, ("127.0.0.1", GATEWAY_PORT))

                probes[msg_id] = {
                    "probe_index": probe_index,
                    "send_monotonic": send_monotonic,
                    "deadline": send_monotonic + TIMEOUT_SECONDS,
                    "grace_deadline": (
                        send_monotonic
                        + TIMEOUT_SECONDS
                        + LATE_RESPONSE_GRACE_SECONDS
                    ),
                    "send_timestamp_utc": send_timestamp,
                    "receive_timestamp_utc": "",
                    "outcome": "PENDING",
                    "rtt_ms": "",
                    "late_matching_echoes": 0,
                    "ignored_responses": 0,
                }

                ordered_ids.append(msg_id)

                print(
                    f"{run_id}: sent probe {probe_index:02d}/"
                    f"{PROBES_PER_REPETITION}, msg_id={msg_id}",
                    flush=True,
                )

                next_probe_index += 1
                next_send_time = (
                    start_time
                    + (next_probe_index - 1)
                    * INTER_PROBE_INTERVAL_SECONDS
                )
                now = time.monotonic()

            now = time.monotonic()

            # Mark expired probes as timed out but retain them briefly
            # to count matching echoes that arrive during the grace period.
            for probe in probes.values():
                if (
                    probe["outcome"] == "PENDING"
                    and now >= probe["deadline"]
                ):
                    probe["outcome"] = "TIMEOUT"

                    print(
                        f"{run_id}: probe {probe['probe_index']:02d} TIMEOUT",
                        flush=True,
                    )

            all_sent = next_probe_index > PROBES_PER_REPETITION
            all_finished = all(
                now >= probe["grace_deadline"]
                for probe in probes.values()
            )

            if all_sent and all_finished:
                break

            wake_times = []

            if not all_sent:
                wake_times.append(next_send_time)

            for probe in probes.values():
                if probe["outcome"] == "PENDING":
                    wake_times.append(probe["deadline"])

                if now < probe["grace_deadline"]:
                    wake_times.append(probe["grace_deadline"])

            next_event = min(wake_times) if wake_times else now + 0.05
            wait_seconds = max(0.0, min(0.05, next_event - now))

            readable, _, _ = select.select(
                [sock], [], [], wait_seconds
            )

            if not readable:
                continue

            # Drain all currently available datagrams.
            while True:
                try:
                    data, source_endpoint = sock.recvfrom(
                        MAX_PACKET_SIZE + 1
                    )
                except BlockingIOError:
                    break

                receive_monotonic = time.monotonic()
                response = decode_packet(data)

                if response is None:
                    ignored_unmatched += 1
                    continue

                msg_id = response["msg_id"]
                probe = probes.get(msg_id)

                valid_echo = (
                    response["msg_type"] == "ECHO"
                    and response["src_ip"] == DEST_IP
                    and response["dst_ip"] == HOST_IP
                    and response["payload"] == PAYLOAD
                )

                # FIX: H1 receives returning echoes from R_A's socket,
                # which is bound to GATEWAY_PORT (5002), not H2_PORT (5006).
                correct_udp_source = (
                    source_endpoint == ("127.0.0.1", GATEWAY_PORT)
                )

                if probe is None or not valid_echo or not correct_udp_source:
                    ignored_unmatched += 1
                    continue

                if (
                    probe["outcome"] == "PENDING"
                    and receive_monotonic <= probe["deadline"]
                ):
                    probe["outcome"] = "SUCCESS"
                    probe["rtt_ms"] = round(
                        (
                            receive_monotonic
                            - probe["send_monotonic"]
                        ) * 1000,
                        3,
                    )
                    probe["receive_timestamp_utc"] = utc_now()

                    print(
                        f"{run_id}: probe {probe['probe_index']:02d} "
                        f"SUCCESS, RTT={probe['rtt_ms']} ms",
                        flush=True,
                    )

                elif (
                    probe["outcome"] == "TIMEOUT"
                    and receive_monotonic <= probe["grace_deadline"]
                ):
                    probe["late_matching_echoes"] += 1

                    print(
                        f"{run_id}: probe {probe['probe_index']:02d} "
                        "received a late matching echo",
                        flush=True,
                    )

                else:
                    probe["ignored_responses"] += 1

    rows = []

    for msg_id in ordered_ids:
        probe = probes[msg_id]

        rows.append({
            "run_id": run_id,
            "delay_ms_per_link": condition["delay_ms"],
            "loss_probability_per_link": condition["loss_prob"],
            "repetition": repetition,
            "base_seed": base_seed,
            "router_seeds": json.dumps(router_seeds, sort_keys=True),
            "probe_index": probe["probe_index"],
            "msg_id": msg_id,
            "send_timestamp_utc": probe["send_timestamp_utc"],
            "receive_timestamp_utc": probe["receive_timestamp_utc"],
            "outcome": probe["outcome"],
            "rtt_ms": probe["rtt_ms"],
            "late_matching_echoes": probe["late_matching_echoes"],
            "ignored_responses": (
                probe["ignored_responses"] + ignored_unmatched
            ),
            "timeout_seconds": TIMEOUT_SECONDS,
            "late_response_grace_seconds": LATE_RESPONSE_GRACE_SECONDS,
            "payload": PAYLOAD,
        })

    return rows


# ------------------------------------------------------------
# Per-run summaries
# ------------------------------------------------------------

def summarize_runs(rows):
    """Summarize delivery and successful RTT measurements for each run."""
    groups = {}

    for row in rows:
        key = (
            row["delay_ms_per_link"],
            row["loss_probability_per_link"],
            row["repetition"],
        )
        groups.setdefault(key, []).append(row)

    summaries = []

    for (delay_ms, loss_prob, repetition), group in sorted(groups.items()):
        successful_rtts = [
            float(row["rtt_ms"])
            for row in group
            if row["outcome"] == "SUCCESS"
        ]

        total = len(group)
        successes = len(successful_rtts)

        summaries.append({
            "delay_ms_per_link": delay_ms,
            "loss_probability_per_link": loss_prob,
            "repetition": repetition,
            "probes_sent": total,
            "successes": successes,
            "timeouts": sum(
                row["outcome"] == "TIMEOUT" for row in group
            ),
            "late_responses": sum(
                int(row["late_matching_echoes"]) > 0 for row in group
            ),
            "delivery_fraction": (
                round(successes / total, 4) if total else 0
            ),
            "mean_successful_rtt_ms": (
                round(sum(successful_rtts) / len(successful_rtts), 3)
                if successful_rtts else ""
            ),
            "min_successful_rtt_ms": (
                min(successful_rtts) if successful_rtts else ""
            ),
            "max_successful_rtt_ms": (
                max(successful_rtts) if successful_rtts else ""
            ),
            "successful_rtt_range_ms": (
                round(max(successful_rtts) - min(successful_rtts), 3)
                if successful_rtts else ""
            ),
        })

    return summaries


# ------------------------------------------------------------
# Main experiment workflow
# ------------------------------------------------------------

def main():
    RESULTS_DIR.mkdir(exist_ok=True)
    LOGS_DIR.mkdir(exist_ok=True)

    if not CONFIG_PATH.exists() or not ROUTER_PATH.exists():
        raise RuntimeError(
            "config.json or router.py is missing. "
            "Run this script from the project folder."
        )

    raw_path = RESULTS_DIR / "baseline_raw.csv"
    summary_path = RESULTS_DIR / "baseline_run_summary.csv"

    # Check before touching the configuration or launching any routers.
    refuse_to_overwrite_results(raw_path, summary_path)

    original_config_bytes = CONFIG_PATH.read_bytes()
    baseline_config = json.loads(original_config_bytes.decode("utf-8"))

    all_rows = []
    processes = {}
    handles = {}

    try:
        verify_h2_is_running()

        expected_host_port = baseline_config["nodes"]["H1"]["udp_port"]
        expected_gateway_port = baseline_config["nodes"]["R_A"]["udp_port"]

        if (
            expected_host_port != HOST_PORT
            or expected_gateway_port != GATEWAY_PORT
        ):
            raise RuntimeError(
                "H1 or R_A UDP ports differ from the experiment settings. "
                "Review the script before continuing."
            )

        total_runs = len(CONDITIONS) * REPETITIONS
        run_number = 0

        for condition_index, condition in enumerate(CONDITIONS):
            for repetition in range(1, REPETITIONS + 1):
                run_number += 1
                base_seed = (
                    10000
                    + condition_index * 1000
                    + repetition * 100
                )

                print(
                    f"\nRun {run_number}/{total_runs}: "
                    f"delay={condition['delay_ms']} ms/link, "
                    f"loss={condition['loss_prob']:.0%}/link, "
                    f"repetition={repetition}, base seed={base_seed}",
                    flush=True,
                )

                stop_routers(processes)
                processes = {}

                for handle in handles.values():
                    handle.close()
                handles = {}

                configured = configure_condition(
                    baseline_config,
                    condition["delay_ms"],
                    condition["loss_prob"],
                )
                save_json(CONFIG_PATH, configured)

                processes, handles, router_seeds = start_routers(
                    base_seed, configured
                )

                rows = run_probes(
                    condition,
                    repetition,
                    base_seed,
                    router_seeds,
                )
                all_rows.extend(rows)

                # Save after each repetition to preserve completed measurements.
                write_csv(raw_path, all_rows, RAW_FIELDS)
                write_csv(
                    summary_path,
                    summarize_runs(all_rows),
                    SUMMARY_FIELDS,
                )

                stop_routers(processes)
                processes = {}

                for handle in handles.values():
                    handle.close()
                handles = {}

        print("\nAll experiment runs completed.")
        print(f"Raw measurements: {raw_path}")
        print(f"Per-run summary: {summary_path}")
        print(f"Total probes recorded: {len(all_rows)}")

    finally:
        stop_routers(processes)

        for handle in handles.values():
            handle.close()

        # Restore the exact original configuration even if a run fails.
        CONFIG_PATH.write_bytes(original_config_bytes)
        print("Original config.json restored.", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Run the Mini Internet baseline experiment: "
            "12 runs across four delay/loss conditions."
        )
    )
    parser.parse_args()

    try:
        main()
    except (
        OSError,
        RuntimeError,
        ValueError,
        KeyError,
        json.JSONDecodeError,
    ) as error:
        print(f"Experiment error: {error}", file=sys.stderr)
        sys.exit(1)