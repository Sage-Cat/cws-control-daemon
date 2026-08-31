# CWS Control Daemon

`cws_control_daemon` is a standalone, reusable implementation for producing
auditable cooperative measurement decisions from caller-supplied evidence.
`software/cws-control-daemon` contains the controller;
`software/failure-aware-timing-validation` contains a reusable failure-campaign
runner and analysis helper.

The implementation includes a hash-linked, side-effect-free M1--M4 reference
path, bounded source ordering before timing validation, and strict validation of
collector-owned measurement-rate transactions. Unit and synthetic replay tests
exercise those software boundaries only. The repository makes no claim about
physical effectiveness, deployment authorization, or production-network safety.

This public repository contains implementation, examples, tests, and portable
documentation only. Concrete experiments, runs, datasets, location records,
credentials, and publication evidence are deliberately outside its boundary.

The repository is licensed under Apache License 2.0; see `LICENSE` and
`NOTICE`.

Run the repository checks before proposing a change:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 scripts/check_public_tree.py
PYTHONDONTWRITEBYTECODE=1 python3 scripts/validate_repository.py
```
