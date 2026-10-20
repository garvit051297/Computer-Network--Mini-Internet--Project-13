# Mini Internet Project

## 1. Overview

This project implements a small virtual network using Python and UDP sockets. Hosts send virtual packets through routers that use static forwarding tables. Packets contain virtual source and destination addresses, a message type, message ID, hop limit, and payload.

The current topology includes two hosts (H1 and H2) and four routers (R_A, R_B, R_C, and R_D). Two paths connect the hosts through the router network.

## 2. Requirements

- Python 3
- PowerShell on Windows
- All project files in the same project directory
- UDP ports configured in `config.json` available for the processes being started

No third-party Python packages are currently required by the documented commands.

## 3. Project Files

- `config.json` — node definitions, UDP ports, links, delay/loss settings, and static forwarding tables.
- `packet.py` — packet encoding and decoding.
- `node.py` — shared node functionality.
- `router.py` — router startup, packet forwarding, hop-limit handling, and configurable link delay/loss.
- `host.py` — host behavior for sending probes and receiving echoes.
- `test_forwarding.py` — standalone forwarding test script.
- `evidence/` — screenshots of configuration, routing, communication, and test results.
- `logs/` — runtime logs generated during testing.

## 4. Topology and Addressing

| Node | Type | Virtual IP | UDP port | Gateway |
|---|---|---|---:|---|
| H1 | Host | 10.0.0.1 | 5001 | R_A |
| R_A | Router | 10.0.0.2 | 5002 | — |
| R_B | Router | 10.0.0.3 | 5003 | — |
| R_C | Router | 10.0.0.4 | 5004 | — |
| R_D | Router | 10.0.0.5 | 5005 | — |
| H2 | Host | 10.0.0.6 | 5006 | R_D |

The configured links are H1–R_A, R_A–R_B, R_A–R_C, R_B–R_D, R_C–R_D, and R_D–H2.

The baseline configuration uses a 5 ms delay and a packet-loss probability of 0.0 on every link.

## 5. Configuration

Edit `config.json` to change node settings, forwarding tables, or link parameters.

- `delay_ms` specifies the configured link delay in milliseconds.
- `loss_prob` specifies the configured probability of simulated packet loss, from 0.0 to 1.0.
- `forwarding_tables` defines each router's next hop for destination virtual IP addresses.

Restore the baseline settings before baseline experiments: use 5 ms delay and 0.0 loss probability on every link.

## 6. Running the Network

Open a separate PowerShell window for each process. Run commands from the project directory:

```powershell
python .\router.py R_A
python .\router.py R_B
python .\router.py R_C
python .\router.py R_D
python .\host.py H2
python .\host.py H1
```

Start each command in its own window and leave the processes running during the test. Start routers before hosts. Check `host.py` for the available host command-line options and probe behavior before using additional arguments.

The configured UDP ports must be available. If a port is occupied, identify the owning process before taking action; do not terminate unrelated services.

## 7. Forwarding Test

The standalone forwarding test requires R_A to be running and UDP port 5003 to be free. It binds a temporary receiver to port 5003, sends a test packet to R_A on port 5002, and checks the forwarded packet.

Run:

```powershell
python .\test_forwarding.py
```

The test checks the virtual source and destination addresses, message type, message ID, payload preservation, and hop-limit decrement from 10 to 9.

This test verifies a single forwarding operation. It does not replace end-to-end multihop, return-path, loop, malformed-packet, or baseline performance tests.

## 8. Evidence and Reproducibility

Screenshots are stored in `evidence/`. Runtime logs may be stored in `logs/`. Review the relevant screenshots and logs when reproducing a result, and preserve the configuration used for each experiment.

For baseline experiments, record the delay and loss settings, random seed, probe count, repetition, delivery fraction, successful-probe RTTs, timeouts, and summary statistics. Keep deterministic fault-injection tests separate from random-loss experiments.

## 9. Current Status and Remaining Work

Verified checks include Python compilation, configuration loading, bidirectional link-settings lookup, Git whitespace checking, and the standalone single-router forwarding test.

Previously captured evidence also documents end-to-end probe/echo traffic and an alternative route through R_C. Distinguish those historical results from tests rerun against the current code.

Remaining work includes completing and documenting the full acceptance tests, checking loop and hop-limit handling, malformed-packet and unknown-destination behavior, testing delay and loss at runtime, conducting repeated baseline experiments, and preparing the final plots, packet traces, and reports.
