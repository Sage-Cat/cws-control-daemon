# CWS control daemon

`cws-control` evaluates CSI envelopes, timing validity, and read-only network context to recommend or apply measurement rates. It consumes collector-owned serial data and never changes network association, radio channel, credentials, or QoS settings. Any hardware or vendor integration remains deployment dependent and is not implied by this portable source tree.

Copy `config/two-node.example.json` or `config/two-node-apply.example.json` outside version control, replace every `REPLACE_*` value, and install it as `/etc/cws-control/config.json`. The examples use role-based source IDs and a generic 40 Hz hardware profile. The apply example preserves local and remote collector command vectors; configure its SSH principal, host, and key files only in the external deployment configuration. `config/method-blocks.example.json` is a reusable randomized-comparison configuration.

Network listeners default to loopback. Binding an evidence or telemetry
receiver to another interface is a deployment-owned security decision and
requires an isolated interface or an authenticated tunnel and firewall.

For reproducible local use, install this package into an isolated environment
from this directory (the project has no third-party runtime dependencies), then
invoke its declared `cws-control` entry point. For a Raspberry Pi service
deployment, a privileged operator supplies a reviewed external configuration
and runs `sudo ./scripts/install-pi-controller.sh . /path/to/config.json`; the
installer creates `/opt/cws-control/venv`, installs this package, and enables
the controller and read-only context services. It does not install, configure,
or validate the separate collector or ESP32 firmware means.

```sh
python3 -m venv --system-site-packages /tmp/cws-control-venv
/tmp/cws-control-venv/bin/pip install --no-deps --no-build-isolation .
/tmp/cws-control-venv/bin/cws-control validate-config --config config/two-node.example.json
/tmp/cws-control-venv/bin/cws-control replay --config config/two-node.example.json --input envelopes.ndjson
/tmp/cws-control-venv/bin/cws-control reference-pipeline --input reviewed-pipeline-input.json --output pipeline-run.json --state-input sacm-state.json --state-output next-sacm-state.json
/tmp/cws-control-venv/bin/cws-control reference-serve --socket /run/user/$(id -u)/cws-reference.sock --state /path/to/sacm-state.json --history /path/to/derived-runs.ndjson
/tmp/cws-control-venv/bin/cws-control serve --config config/two-node.example.json --output decisions.ndjson
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -v
```

`prplos/` contains a read-only telemetry agent and its service scripts. Supply the endpoint, node ID, and token at installation; no token is stored here.

## Versioned reference/replay core

`cws_control.contracts` and `cws_control.pipeline` implement a stdlib-only,
side-effect-free M1 → M4 reference path for synthetic and retained replay
inputs. It hash-links `cws-source-record/2`, M1 evidence/admission, a supplied
hash-verified M2 model artifact, BM-CSF/QISS candidate selection, and SACM
accepted state. It neither opens evidence files nor performs network, hardware,
or actuation operations. The core is replay-tested with synthetic fixtures;
it is **not physically validated** and is not a reproduction of any historical
execution-time result. A live caller must explicitly invoke this same pure path
with separately authorized, qualified evidence.

The `reference-pipeline` command is the file boundary around that exact pure
function. It validates an optional hash-covered SACM state, atomically writes
the derived run ledger and next state, and does not copy raw input records into
its output. `reference-serve` calls the same request executor and exact pure
function through a filesystem AF_UNIX socket. It is sequential, creates the
socket with mode `0600`, bounds each request, accepts one JSON object and emits
one JSON response per connection, and can atomically retain SACM state plus an
append-only history of derived outputs. It refuses an existing socket path and
never stores request records or caller-only notes in the history. `--max-requests`
is available for bounded local validation.

The local service does not discover, derive, qualify, or authorize evidence,
model artifacts, candidates, decisions, or actuation. Its request remains the
reviewed `m1`, `m2`, `m3`, optional `m4_candidate`, and optional `parameters`
object accepted by the one-shot boundary; qualified artifacts remain the
caller's responsibility. The operational UDP `serve` loop is still separate
and does **not** automatically feed the reference service or claim to run the
reference M1--M4 path on live traffic.

Operational ingestion accepts the legacy `cws-source-record/1` envelope and
strictly validates `cws-source-record/2`, including its raw-record SHA-256 and
nullable device/boot/configuration epochs. Each source has a bounded,
record-count-based reorder window before parsing and timing mutation. Gaps,
duplicates, late records, resets, reconnects, buffered rows, and bounded recent
ordering events are included in every decision ledger.

In explicit `apply` mode, an actuator exit status is not an acknowledgement.
The daemon accepts an action only when collector stdout is a matching
`cws-actuation-transaction/1` committed result whose terminal facts hash,
phase/deadline/reply correlations, state hashes, and postcondition all verify.
The data contract and live actuator share one validator. It admits both the
normal APPLY→VERIFY commit and the bounded APPLY-timeout→QUERY-timeout→GET_STATE
recovery when the final state read independently proves the requested state;
the two phase shapes are validated separately.
Version 1 remains the legacy operational record. Version 2 additionally binds
the top-level transaction, terminal facts, and requested firmware state to one
expected M4 decision SHA-256 and transaction ID. This verifies provenance
correlation only; it is not deployment authorization or physical validation.
Partial multi-node actions remain pending and are retried by a later decision.
This boundary is unit-tested with mocked subprocesses; no physical actuator has
been validated by these tests.

## Backend-neutral future adapter protocol

`cws_control.adapter_protocol` defines a local, in-process version-1 contract
and runner boundary for possible future OpenWrt, prplMesh, or EasyMesh adapters.
It contains **no real adapter**, confirmed vendor API, command vector, network
client, hardware action, or adapter-protocol CLI endpoint. Backend kind is only a
classification. Exact operation IDs and all parameter/state objects are
adapter-defined opaque data whose complete canonical JSON is hash-covered; the
portable layer does not assign them operational meaning.

The runner defaults requests to `observe`. `dry-run` may invoke only the
adapter's non-mutating read and preview methods. `apply` requires an exact
allowlisted operation, matching measurement-control or traffic/topology
boundary, adapter-declared apply capability, exact pre-state hash, expected
post-state hash, and the explicit `allow_apply=True` software latch. That latch
is a local programming guard only: it is not evidence of operator, experiment,
deployment, or production authorization. Every in-process call receives a
bounded timeout and monotonic deadline; implementations must honor it because
the runner cannot safely preempt arbitrary in-process code.

After apply, a fresh readback must match the requested postcondition hash. On
adapter error, timeout, deadline overrun, or mismatch, the runner attempts to
restore the captured opaque pre-state and reads it back, reporting either
`rolled-back` or `rollback-failed`. Transactions retain only hashes and typed
phase facts. Synthetic in-memory tests establish protocol sequencing only, not
the existence, correctness, authorization, or physical safety of any backend.

The live CSI mirror uses unauthenticated UDP, and the prplOS telemetry receiver
uses shared-token authentication over plaintext HTTP. Bind them only to an
isolated experiment interface or protect them with an authenticated tunnel and
firewall. `apply` now fails closed unless `evidence_transport` is the versioned
`loopback-authenticated-tunnel/1` or `isolated-experiment-interface/1` profile;
it also rejects wildcard listeners, and the tunnel profile must bind to a
loopback address. This validates the local configuration boundary, not the
existence or correctness of an external tunnel/firewall. Do not expose either
listener to an untrusted network.
