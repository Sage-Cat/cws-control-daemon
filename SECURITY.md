# Security and privacy

Do not commit credentials, tokens, private keys, concrete network identifiers,
hostnames, board serials, device paths, exact site details, participant data,
raw router snapshots, raw captures, or private control arguments.

Under `software/`, keep deployable behavior parameterized through explicit
environment variables, command-line arguments, or reviewed example files. Do
not replace placeholders with live values in Git; keep real deployment maps,
tokens, SSH state, and serial identities outside the repository.

Do not commit experiment chunks, controller/context streams, deployment
reports, or a measured replay ledger. Keep them with the authorized experiment
owner. This repository's tests must use synthetic fixtures only.

The live CSI mirror is unauthenticated UDP. The prplOS telemetry receiver uses
a shared token over plaintext HTTP; the token authenticates requests but does
not provide transport confidentiality. Restrict both listeners to an isolated
experiment interface or place them behind an authenticated tunnel and firewall.
Never expose them to an untrusted network or enable `apply` while evidence can
be injected over an unauthenticated path.

If an artifact exposes private or security-sensitive content, stop and remove
it from the proposed public change. Do not weaken the tracked-path policy to
admit it.
