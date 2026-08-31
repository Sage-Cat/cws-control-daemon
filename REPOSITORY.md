# Repository boundary

This repository owns the portable controller, its reusable failure-aware timing
validation helper, tests, examples, and documentation of their software
contracts.

It does not own a deployment, a physical site, a concrete run, a raw or derived
dataset, a publication artifact, or credentials. A separate authorized
experiment owns each concrete run and result; a separate location record owns
unchanged site facts. Public exports require their own explicit sanitization and
authorization.

`public-release.toml` is the authoritative release-boundary policy. Its
deterministic checker examines tracked paths only; it intentionally does not
pretend that heuristic text scanning can prove privacy.
