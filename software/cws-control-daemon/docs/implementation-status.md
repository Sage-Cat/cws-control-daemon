# Portable implementation boundary

## Versioned reference pipeline: implemented, replay-tested

The package now contains a pure, stdlib-only reference pipeline for M1 evidence
admission, M2 descriptor fusion using only a supplied hash-verified model
artifact, M3 BM-CSF with the QISS classical ordering variant, and stateful M4
SACM. Canonical compact JSON hashes, required fields, unknown major versions,
and broken correlations fail closed. Golden tests use synthetic inputs only.

This is implementation and synthetic replay validation, not physical validation
or a claim of historical-result reproduction. No model weights, private records,
hardware outcomes, production WLAN mutation, or causal service result is
included or implied.

The controller package, collector command protocol, timing gate, replay path, and read-only prplOS telemetry receiver are implemented and unit tested. Hardware topology, access-point counter availability, service paths, and real deployment outcomes are configuration-dependent. This source tree does not carry measured deployment logs or any claim that controller recommendations changed a production network.

## Safe live/replay integration: implemented, unit-tested

- Collector source records remain v1-readable. Version 2 is parsed strictly,
  including raw payload hash verification and explicit parser/device epoch
  facts; unknown majors and broken hashes fail closed.
- Per-source ordering is bounded by record count and runs before telemetry
  parsing or timing updates. It deterministically drains contiguous records,
  records gaps/duplicates/late arrivals/session resets/reconnects, flushes on a
  context change, rejects retained stale contexts, and resets timing at a gap
  or context discontinuity.
- `reference-pipeline` invokes the same pure `run_reference_pipeline` function,
  emits a hash-covered derived ledger, and can atomically restore/persist the
  hash-verified SACM state.
- `reference-serve` reuses the same request executor and exact reference core on
  a mode-`0600` filesystem Unix socket. Requests are sequential and size-bounded;
  optional state is replaced atomically and optional history appends only the
  same hash-covered derived output produced by the one-shot command. Malformed,
  oversized, schema-invalid, and duplicate-state requests return typed errors
  without changing SACM state, and a pre-existing socket path is never removed.
- Apply-mode subprocess success requires a matching committed collector
  transaction with canonical terminal-facts hash, complete phase/deadline/reply
  correlations, state hashes, and a verified postcondition. A zero process exit
  alone is a failure; a partial multi-node action is not recorded as applied.
- Apply mode also rejects unauthenticated or wildcard CSI evidence listeners.
  Its versioned transport profile permits only an explicit isolated experiment
  interface or a loopback listener behind an operator-provided authenticated
  tunnel; the daemon does not claim to validate that external isolation.
  One shared validator covers the collector's actual terminal status schema.
  Its normal APPLY/VERIFY path and its explicit timeout QUERY/recover-state path
  have separate fail-closed correlation requirements.
  The backward-compatible version-2 contract also requires the same M4
  decision SHA-256 in the transaction, terminal facts, and requested-state
  hash. Supplying an expected decision forbids fallback to version 1. This is a
  verified provenance link, not actuation authority or physical validation.
- A separate backend-neutral adapter protocol now validates versioned
  capability, request, and transaction records for future OpenWrt, prplMesh,
  or EasyMesh work. It keeps exact operation IDs and backend parameters/state
  opaque and fully hash-covered, separates measurement-control from
  traffic/topology boundaries, and uses an explicit exact-ID allowlist.
  Observe is the request default; dry-run cannot enter the mutation method.
  Apply additionally requires adapter capability, exact pre/post state hashes,
  and `allow_apply=True`, followed by verified readback or a rollback/readback
  classification. The latch is not external authorization. No real adapter,
  confirmed backend API, adapter-protocol CLI endpoint, subprocess, network
  call, or hardware action is implemented by this protocol.

These paths are unit/replay tested only. The local reference service provides
call parity, not automatic evidence/model/candidate derivation, authorization,
or actuation. The live UDP `serve` command is not automatically supplied with
qualified model artifacts or candidate universes, and no physical transaction,
production WLAN change, or scientific result is established here.
