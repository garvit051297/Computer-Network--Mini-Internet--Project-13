# Mini Internet Project

A Python-based miniature Internet simulation demonstrating custom packet forwarding, static routing, network delay and packet loss, reliable UDP data transfer, CRC-32 error detection, and experimental network performance analysis.

## 1. Project Overview

This project implements a miniature virtual Internet using Python and UDP sockets. It demonstrates how hosts communicate through intermediate routers, how routers forward packets using static forwarding tables, and how network delay and packet loss affect communication.

The network uses a diamond topology consisting of two hosts and four routers, with two possible paths between the hosts:

- **Upper path:** H1 → R_A → R_B → R_D → H2
- **Lower path:** H1 → R_A → R_C → R_D → H2

The project implements two communication approaches:

1. **Best-effort forwarding:** Packets are forwarded through the network without a guarantee of delivery or retransmission.
2. **Reliable UDP transfer (Track B):** A stop-and-wait protocol adds acknowledgments, sequence numbers, timeout-based retransmissions, duplicate detection, and ordered application-level delivery.

The reliable-transfer protocol also incorporates CRC-32-based error detection to identify unintended changes to DATA messages during transmission.

The project includes automated tests, controlled fault-injection tests, quantitative experiments, CSV results, supporting evidence, and Wireshark packet captures.

## 2. Requirements and Setup

### 2.1 Requirements

- Python 3 (developed and tested with Python 3.13.1).
- Windows and PowerShell.
- Git for cloning the repository.
- Required UDP ports available.
- All project scripts and configuration files maintained in the project directory.

The implementation uses Python standard-library modules and does not require third-party Python packages for the documented commands.

### 2.2 Clone the repository

Clone the project from the `master` branch:

```powershell
git clone --branch master https://github.com/garvit051297/Computer-Network--Mini-Internet--Project-13.git
cd Computer-Network--Mini-Internet--Project-13
```

The explicit branch selection ensures that the project implementation is obtained from `master`, rather than the separate README-only history currently associated with `main`.

### 2.3 Verify the Python installation

```powershell
python --version
```

The project was developed and tested using Python 3.13.1.

## 3. Project Structure

### 3.1 Main implementation files

| File | Description |
|---|---|
| `packet.py` | Defines packet encoding, decoding, header fields, and packet-size limits. |
| `node.py` | Provides shared node and UDP communication functionality. |
| `host.py` | Implements host behavior, including best-effort and reliable-transfer modes. |
| `router.py` | Implements packet reception, validation, forwarding, delay, and simulated packet loss. |
| `config.json` | Defines virtual addresses, UDP ports, network links, link settings, and forwarding tables. |
| `reliable_protocol.py` | Defines reliable DATA and ACK messages, validation, and CRC-32 calculation and verification. |
| `reliable_transfer.py` | Implements the stop-and-wait reliable-transfer sender and receiver. |

### 3.2 Testing and acceptance files

| File | Description |
|---|---|
| `test_forwarding.py` | Tests basic router forwarding and packet-field behavior. |
| `bidirectional_acceptance_test.py` | Tests bidirectional communication between H1 and H2. |
| `test_router_forwarding_errors.py` | Tests invalid and nonadjacent next-hop handling. |
| `test_reliable_protocol.py` | Tests reliable-protocol message encoding, decoding, validation, and integrity behavior. |
| `test_reliable_transfer.py` | Tests reliable-transfer behavior, including retransmission and duplicate handling. |

### 3.3 Experiment and supporting files

| File | Description |
|---|---|
| `experiment.py` | Runs the supplementary all-links baseline experiment. |
| `experiment_selected_link.py` | Runs the primary baseline experiment by varying the R_B–R_D link pair. |
| `reliable_experiment.py` | Runs the supplementary all-links reliable-transfer comparison. |
| `reliable_experiment_selected_link.py` | Runs the primary selected-link comparison of best-effort and reliable transfer. |
| `diagnose_probe.py` | Provides supporting diagnostic functionality. |
| `router_before_delay_loss.py` | Preserved earlier router implementation; the active network uses `router.py`. |
| `router.py.before_seed` | Preserved earlier router implementation. |

### 3.4 Supporting folders

| Folder | Contents |
|---|---|
| `captures/` | Wireshark packet captures (`.pcapng`). |
| `evidence/` | Screenshots and other project verification evidence. |
| `logs/` | Retained text-based project records. Generated runtime and experiment `.log` files are excluded from Git by `.gitignore`. |
| `results/` | Raw experiment data and aggregated CSV summaries. |

Historical configuration snapshots are also retained at the project root:

- `config_baseline_S4.json`
- `config_before_forced_drop.json`
- `config_before_loop_test.json`
- `config_before_loss_test.json`

The active network configuration is `config.json`.

## 4. Network Topology and Addressing

The network contains six nodes: two hosts and four routers.

| Node | Virtual IP Address | UDP Port | Gateway |
|---|---|---:|---|
| H1 | 10.0.0.1 | 5001 | R_A |
| R_A | 10.0.0.2 | 5002 | — |
| R_B | 10.0.0.3 | 5003 | — |
| R_C | 10.0.0.4 | 5004 | — |
| R_D | 10.0.0.5 | 5005 | — |
| H2 | 10.0.0.6 | 5006 | R_D |

The configured network links are:

- H1 – R_A
- R_A – R_B
- R_A – R_C
- R_B – R_D
- R_C – R_D
- R_D – H2

The baseline configuration uses a 5 ms delay and 0% packet loss on each link.

The virtual IP addresses are carried in the custom packet header. Actual local communication between processes takes place through UDP sockets using the configured localhost ports. Consequently, the virtual network addresses and the underlying UDP endpoints represent two different addressing layers.

## 5. Packet Format and Router Forwarding

### 5.1 Custom packet format

The project uses a custom packet format with a fixed 45-byte header.

| Header Field | Size |
|---|---:|
| Source virtual IP | 16 bytes |
| Destination virtual IP | 16 bytes |
| Hop limit | 1 byte |
| Message type | 8 bytes |
| Message ID | 4 bytes |
| **Total header size** | **45 bytes** |

The payload follows the header. The maximum packet size is 1,200 bytes.

### 5.2 Static forwarding

Each router uses a static forwarding table to determine the next hop for a packet based on its destination virtual IP address.

The forwarding implementation checks:

- Whether the packet is valid.
- Whether the destination is known.
- Whether the selected next hop exists.
- Whether the next hop is a directly connected neighbor.
- Whether the packet has an available hop limit.

When forwarding a packet, the router decrements its hop limit. A packet whose hop limit is exhausted is dropped to prevent indefinite forwarding loops.

### 5.3 Error handling

The router includes handling for:

- Malformed or oversized packets.
- Unknown destinations.
- Invalid next hops.
- Next hops that are not directly connected neighbors.
- Exhausted hop limits.
- Simulated link packet loss.

These conditions are recorded in router logs to support troubleshooting and verification. Malformed and oversized packets share a rejection category in the current implementation.

### 5.4 Link configuration

The `config.json` file defines the network links and their settings.

- `delay_ms` specifies the configured link delay in milliseconds.
- `loss_prob` specifies the probability of simulated packet loss, from 0.0 to 1.0.
- `forwarding_tables` defines each router's next hop for destination virtual IP addresses.

Restore the baseline settings before baseline experiments: use 5 ms delay and 0.0 loss probability on every link.

## 6. Running the Network

Each router and host should run in its own PowerShell window. Run all commands from the project directory.

### 6.1 Start the routers

Open four PowerShell windows.

**Window 1 – R_A**

```powershell
python .\router.py R_A
```

**Window 2 – R_B**

```powershell
python .\router.py R_B
```

**Window 3 – R_C**

```powershell
python .\router.py R_C
```

**Window 4 – R_D**

```powershell
python .\router.py R_D
```

Wait for all four routers to start successfully.

### 6.2 Start the hosts

Open two additional PowerShell windows.

**H2**

```powershell
python .\host.py H2
```

**H1**

```powershell
python .\host.py H1
```

The hosts can now exchange best-effort messages through the configured network. H1 initiates a probe, and H2 provides the corresponding echo response.

### 6.3 Reliable host modes

The host implementation also supports dedicated reliable-transfer modes.

Start the receiver on H2:

```powershell
python .\host.py H2 reliable-receive
```

Then start the sender on H1:

```powershell
python .\host.py H1 reliable-send "Message 1" "Message 2"
```

The receiver should be started before the sender. The sender accepts a finite sequence of messages for reliable delivery.

## 7. Track B: Reliable UDP Transfer Implementation

### 7.1 Motivation

UDP provides connectionless data transmission but does not guarantee that packets will arrive, arrive in order, or arrive only once.

To address these limitations, Track B implements a stop-and-wait reliable-transfer protocol on top of the project's UDP-based network.

The protocol provides ordered application-level delivery through acknowledgments, sequence numbers, retransmissions, and duplicate detection.

### 7.2 DATA and ACK messages

The protocol uses two principal message types:

- **DATA:** Carries an application message from the sender to the receiver.
- **ACK:** Confirms receipt of a DATA message.

Each DATA message contains:

- A transfer identifier (`transfer_id`).
- A sequence number (`seq`).
- The application data.
- A final-message indicator (`is_last`).
- A CRC-32 checksum.

ACK messages identify the transfer and sequence number being acknowledged.

The transfer identifier distinguishes one transfer from another, while sequence numbers identify the individual messages within a transfer.

### 7.3 Stop-and-wait operation

The sender transmits one DATA message and waits for its corresponding ACK before sending the next message.

The process is:

1. The sender prepares the next DATA message and assigns its transfer identifier and sequence number.
2. The sender transmits the DATA packet through the UDP network.
3. The receiver validates the DATA message and checks its sequence number and checksum.
4. If the message is valid and is the expected next message, the receiver delivers it to the application and sends an ACK.
5. The sender receives the matching ACK and advances to the next sequence number.
6. If the ACK does not arrive before the timeout, the sender retransmits the DATA message.
7. The process continues until all messages have been acknowledged or the retry limit is exhausted.

Only one message can be awaiting acknowledgment at a time. This is the defining characteristic of the stop-and-wait approach.

### 7.4 Timeout and retransmission

The sender uses an acknowledgment timeout to determine when a message should be retransmitted.

The selected-link experiment uses:

- ACK timeout: 0.5 seconds.
- Maximum retransmissions: 5 per message.

If an ACK is not received within the timeout, the sender retransmits the outstanding DATA message.

The sender records transmission attempts and retransmission events. If the maximum retry limit is reached without a matching ACK, the transfer reports an explicit failure rather than waiting indefinitely.

### 7.5 Duplicate detection and ordered delivery

Retransmission can cause the receiver to receive the same DATA message more than once. This can happen, for example, when the original DATA arrives successfully but its ACK is lost.

To prevent duplicate application-level delivery, the receiver tracks the expected sequence number for each transfer.

- A message with the expected sequence number is accepted and delivered.
- A previously received message is recognized as a duplicate and is not delivered again.
- The receiver can acknowledge a duplicate so that the sender can stop retransmitting it.

This mechanism maintains ordered, exactly-once application-level delivery for successfully completed transfers, despite possible duplicate packet arrivals.

### 7.6 Final-message and receiver-grace handling

The final DATA message is identified using the `is_last` flag.

A special case occurs when the receiver gets the final DATA message and sends its ACK, but the ACK is lost. The sender may retransmit the final DATA message even though the receiver has already completed delivery.

The receiver therefore retains completed-transfer information during a grace period. If it receives a duplicate final DATA message for a completed transfer, it recognizes the duplicate and sends the ACK again without delivering the application message a second time.

The selected-link experiment uses a 15-second receiver grace period. This allows final retransmissions and acknowledgments to be handled after the application data has been delivered.

### 7.7 Reliable-transfer fault scenarios

The implementation and project evidence cover the following scenarios:

| Scenario | Expected behavior |
|---|---|
| No packet loss | Messages are acknowledged and delivered in sequence without retransmission. |
| DATA packet loss | The sender times out and retransmits the missing DATA message. |
| ACK packet loss | The sender retransmits DATA; the receiver recognizes the duplicate and re-ACKs it. |
| Final ACK loss | The receiver recognizes the duplicate final DATA message and sends the final ACK again without duplicate delivery. |
| Unreachable destination | The sender stops after the retry limit and reports an explicit failure. |

These tests demonstrate why acknowledgments and retransmissions are necessary when building reliable communication on top of UDP.

## 8. CRC-32 Error Detection

### 8.1 Purpose

In addition to reliable delivery, the project implements CRC-32-based error detection for reliable DATA messages.

Reliability and error detection address different problems:

- **Reliability** addresses packet loss and missing acknowledgments through retransmission.
- **Error detection** checks whether the received DATA message matches the contents originally transmitted.

A packet can arrive at the receiver but contain altered data. CRC-32 provides a mechanism for detecting such changes.

### 8.2 CRC-32 calculation

The implementation uses Python's standard-library `zlib.crc32()` function.

For reliable DATA messages, the checksum is calculated over a compact JSON representation of these four fields:

1. `transfer_id`
2. `seq`
3. `data`
4. `is_last`

The JSON representation uses UTF-8 encoding, `ensure_ascii=False`, and compact separators to produce a consistent checksum input.

The CRC-32 result is masked to 32 bits and represented as an eight-character hexadecimal string.

The protocol therefore protects not only the application data but also the transfer identity, sequence number, and final-message indicator.

### 8.3 Encoding and verification

When the sender prepares a DATA message:

1. It constructs the DATA fields.
2. It calculates the CRC-32 checksum over the four fields.
3. It includes the resulting hexadecimal checksum in the encoded DATA payload.
4. It transmits the packet through the network.

When the receiver processes the DATA message:

1. It decodes the received payload.
2. It validates the required DATA fields and checksum format.
3. It independently recalculates the CRC-32 checksum.
4. It compares the calculated checksum with the checksum included in the received message.
5. If the values match, the message passes error detection.
6. If the values differ, the receiver rejects the DATA message and reports `checksum_mismatch`.

The receiver does not accept a DATA message with a checksum mismatch as valid application data.

### 8.4 Interaction with reliable retransmission

CRC-32 verification works together with the stop-and-wait protocol.

If a DATA message fails error detection, the receiver does not accept it as a valid message. The sender does not receive the expected successful acknowledgment and will eventually retransmit the outstanding DATA message after its timeout.

This allows the protocol to recover when a retransmission delivers a valid copy of the message.

The project includes a controlled corruption test demonstrating that the receiver detects a checksum mismatch and that a subsequent valid retransmission can be accepted.

### 8.5 Scope and limitations

CRC-32 is an error-detection mechanism. It is useful for detecting accidental data alteration, but it is not encryption, authentication, or a cryptographic integrity guarantee.

The checksum does not prevent a malicious party from deliberately modifying a message and recalculating its CRC-32 value.

CRC-32 is therefore an additional error-detection feature, not a replacement for the underlying reliability protocol.

## 9. Automated Testing and Acceptance Verification

### 9.1 Complete unit-test suite

Run the complete test suite:

```powershell
python -m unittest discover -v
```

The verified suite contains 30 passing tests:

- 25 reliable-protocol tests.
- 5 reliable-transfer tests.

### 9.2 Forwarding-error acceptance tests

Run:

```powershell
python .\test_router_forwarding_errors.py
```

This test verifies two forwarding-error cases:

- A forwarding-table entry that references a nonexistent next hop.
- A forwarding-table entry that references an existing but nonadjacent next hop.

The verified result was 2/2 passing checks.

### 9.3 Standalone forwarding test

The standalone forwarding test is:

```powershell
python .\test_forwarding.py
```

It checks packet forwarding through R_A, including packet fields and hop-limit decrement from 10 to 9.

The test requires R_A to be running and the receiver endpoint on UDP port 5003 to be available. Ensure that the port is not occupied by an unrelated process before running it.

### 9.4 Bidirectional acceptance test

The bidirectional acceptance test is implemented in:

`bidirectional_acceptance_test.py`

Its recorded results are stored in:

`results/bidirectional_acceptance_results.csv`

The recorded acceptance run contains:

- 20 successful messages from H1 to H2.
- 20 successful messages from H2 to H1.

### 9.5 Additional fault and protocol verification

The project also retains evidence and logs for:

- Forwarding through both available paths.
- Hop-limit exhaustion and forwarding-loop prevention.
- Unknown-destination handling.
- Malformed-packet rejection and router continued operation.
- Invalid and nonadjacent next-hop handling.
- Explicit host timeout behavior.
- Reliable DATA-loss recovery.
- Reliable ACK-loss and final-ACK-loss recovery.
- Bounded retry failure.
- CRC-32 checksum-mismatch detection and recovery.

The evidence folder and retained logs contain supporting records for these scenarios.

## 10. Experimental Methodology and Results

The project includes two principal experimental studies:

1. Baseline network performance under different link-delay and packet-loss conditions.
2. A comparison of best-effort and reliable UDP transfer under the same selected-link conditions.

The primary experiments vary the R_B–R_D link pair while retaining baseline settings on the other links.

### 10.1 Baseline network experiment

Run:

```powershell
python .\experiment_selected_link.py
```

The experiment uses four conditions:

| R_B–R_D Delay | Configured Loss |
|---:|---:|
| 5 ms | 0% |
| 5 ms | 5% |
| 25 ms | 0% |
| 25 ms | 5% |

Each condition is repeated three times, with 20 probes per run.

This produces:

- 4 experimental conditions.
- 3 repetitions per condition.
- 12 total runs.
- 240 total probes.

**Recorded results**

| Delay | Loss | Successful Probes | Mean RTT |
|---:|---:|---:|---:|
| 5 ms | 0% | 60/60 (100%) | 97.271 ms |
| 5 ms | 5% | 58/60 (96.67%) | 98.454 ms |
| 25 ms | 0% | 60/60 (100%) | 130.434 ms |
| 25 ms | 5% | 52/60 (86.67%) | 130.248 ms |

Overall, 230 of 240 probes succeeded, giving a delivery rate of 95.83%. Ten probes timed out.

The results show that increasing the selected-link delay from 5 ms to 25 ms increased end-to-end RTT by approximately 32–33 ms.

The results also demonstrate that configured link loss is probabilistic. A 5% loss setting on one link does not necessarily produce exactly 5% end-to-end packet loss.

**Primary result files**

- `results/baseline_selected_link_raw.csv`
- `results/baseline_selected_link_run_summary.csv`
- `results/baseline_selected_link_condition_summary.csv`

### 10.2 Track B: Best-effort versus reliable-transfer experiment

Run:

```powershell
python .\reliable_experiment_selected_link.py
```

The experiment compares best-effort and reliable transfer under the same four selected-link delay/loss conditions.

Each protocol is evaluated using three repetitions per condition and 20 messages per transfer.

This produces:

- 4 experimental conditions.
- 3 repetitions per condition.
- 2 protocols.
- 24 total trials.
- 480 total message transmissions across the two protocols' workloads (240 messages per protocol).

**Experimental settings**

Best-effort protocol:

- Fixed sending interval: 0.30 seconds.
- Approximate scheduled sending rate: 3.33 messages per second.
- Fixed observation window: 2 seconds after the scheduled final send.
- No retransmissions.

Reliable protocol:

- Stop-and-wait, acknowledgment-driven sending.
- ACK timeout: 0.5 seconds.
- Maximum retransmissions: 5 per message.
- Receiver grace period: 15 seconds.

The two protocols use different pacing mechanisms. Consequently, elapsed times should not be interpreted as a direct, equivalent throughput comparison.

**Recorded results**

| Protocol | Delay | Loss | Mean Delivery | Complete Transfers | Mean Retransmissions |
|---|---:|---:|---:|---:|---:|
| Best effort | 5 ms | 0% | 100% | 3/3 | 0 |
| Best effort | 5 ms | 5% | 96.67% | 2/3 | 0 |
| Best effort | 25 ms | 0% | 100% | 3/3 | 0 |
| Best effort | 25 ms | 5% | 93.33% | 0/3 | 0 |
| Reliable | 5 ms | 0% | 100% | 3/3 | 0 |
| Reliable | 5 ms | 5% | 100% | 3/3 | 1.333 |
| Reliable | 25 ms | 0% | 100% | 3/3 | 0 |
| Reliable | 25 ms | 5% | 100% | 3/3 | 4 |

Across the 12 best-effort trials:

- 234 of 240 messages were delivered.
- 8 of 12 transfers completed.
- No retransmissions were performed.

Across the 12 reliable trials:

- 240 of 240 messages were delivered.
- 12 of 12 transfers completed.
- 16 retransmissions were performed.

These results demonstrate the benefit of acknowledgment-based retransmission under simulated packet loss. Best-effort communication can lose messages without recovering them, while the reliable protocol can recover from the tested losses through retransmission.

The reliable protocol achieves complete delivery in all four experimental conditions, but this comes with retransmission and acknowledgment overhead.

**Primary result files**

- `results/track_b_selected_link_full_raw.csv`
- `results/track_b_selected_link_full_summary.csv`

The supplementary all-links experiments and their CSV outputs are retained separately.

## 11. Wireshark Packet Captures

The `captures/` folder contains three packet captures:

| File | Description |
|---|---|
| `mini_internet_bidirectional_capture.pcapng` | Bidirectional network traffic capture. |
| `mini_internet_project_only.pcapng` | Filtered project-only UDP capture. |
| `track_b_retransmission_wireshark.pcapng` | Capture demonstrating reliable-transfer retransmission behavior. |

Wireshark can be used to inspect UDP packets, packet forwarding, and retransmission behavior.

The captures also help demonstrate the difference between:

- Actual UDP source and destination endpoints, represented by the localhost UDP ports.
- Virtual source and destination addresses, carried inside the project's custom packet header.

Router and host logs provide complementary evidence of forwarding decisions, packet drops, and reliable-protocol events.

## 12. Configuration and Reproducibility

The `config.json` file defines:

- Node virtual IP addresses.
- UDP port assignments.
- Network links and adjacency.
- Static forwarding tables.
- Link delay.
- Link packet-loss probability.

For reproducible experiments:

1. Start from the documented baseline configuration.
2. Use the selected-link experiment scripts for the primary quantitative results.
3. Keep the experimental conditions and message/probe counts consistent.
4. Record random seeds and experimental settings.
5. Preserve raw CSV data and aggregated summaries.
6. Keep deterministic fault-injection tests separate from probabilistic experiments.

The primary selected-link experiments modify the R_B–R_D link pair rather than applying the experimental settings to every network link.

## 13. Known Scope and Limitations

- The project is a local UDP-based network simulation, not a deployment across physical Internet routers.
- Routing uses static forwarding tables rather than a dynamic routing protocol.
- Best-effort UDP does not guarantee delivery or ordering.
- Stop-and-wait reliability introduces acknowledgment and retransmission overhead.
- Random packet loss is probabilistic, so results can vary across runs.
- The primary quantitative experiments focus on the R_B–R_D link pair.
- Malformed and oversized packets share a router rejection category; separate oversized-packet runtime acceptance evidence is not claimed.
- CRC-32 detects accidental data changes but does not provide cryptographic authentication or protection against deliberate modification.
- Experiment results depend on the configured timeout, sending interval, observation period, and network conditions.

## 14. Evidence and Project Outputs

The project retains supporting evidence in the following locations:

- `evidence/` – screenshots of network tests, acceptance checks, experiments, and protocol behavior.
- `logs/` – retained text-based project records. Generated runtime and experiment `.log` files are excluded from Git.
- `captures/` – Wireshark packet captures.
- `results/` – raw CSV data and aggregated experiment summaries.

These files provide supporting records for the reported network behavior, reliable-transfer implementation, CRC-32 error-detection feature, and quantitative experimental findings.

They should be retained alongside the source code to support project review, demonstration, and reproducibility.