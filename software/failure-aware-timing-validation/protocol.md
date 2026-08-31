# Failure-aware timing validation protocol

Use at least two independently observed sensing sources. Before every injected
fault, require repeated fresh timing-valid quorum and record the randomized
trial order and seed. Each injection, acknowledgement, detection observation,
restore command, and recovered-quorum boundary must be written to the campaign
ledger. Abort on a failed precondition and restore the reviewed safe sensing
rate or power state.

The reusable trial classes are sensing silence, software reboot, reviewed
hardware reset, reviewed per-source power loss, and operator-confirmed physical
disconnect. Natural-operation runs are separate observational evidence and
must not be labelled as injected failures.

Replay the same envelopes through three fixed comparators: raw timestamp
regression, rollover-aware regression, and failure-aware boot-epoch plus
rollover handling. Report individual trials and grouped detection, timing-fit
recovery, quorum recovery, intervention latency, invalid admissions, precision,
recall, false resets per source-hour, median, IQR, p95, and bootstrap confidence
intervals where the sample size supports them.

No timing experiment establishes occupancy/activity accuracy or a causal QoS
gain unless those outcomes have their own ground truth and protocol.
