# Contributing

This is a standalone public implementation repository.

1. Read `README.md`, `REPOSITORY.md`, `SECURITY.md`, and
   `public-release.toml`.
2. Keep reusable implementation, examples, tests, and portable documentation
   under the existing public surfaces.
3. Keep every concrete run, dataset, observation, deployment map, credential,
   and location fact in its authorized external owner. Do not copy it here.
4. Use role-based example identifiers and replaceable values; do not add host,
   site, participant, device, or network identifiers.
5. Run both test suites, `scripts/check_public_tree.py`, and
   `scripts/validate_repository.py`; review the complete diff before commit.

The tracked-path policy rejects `materials/`, `provenance/`, `experiments/`,
`locations/`, `private/`, and generated relationship/publication views. Do not
weaken that policy to admit a concrete experiment artifact.
