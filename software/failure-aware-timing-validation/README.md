# Failure-aware timing validation helper

This maintained helper runs controlled sensing-source failure campaigns and
derives timing, recovery, quorum, intervention, and ablation metrics. Raw data,
campaign ledgers, and derived results belong to an authorized experiment,
not this repository.

Campaign runners require `--run-dir` to name an existing external run directory
containing `run.toml`; a path inside this repository is refused. They write only below
`results/failure_aware_timing_validation/` in that run. Controller decisions and
the actuation interlock are also explicit CLI inputs; there are no output-path
defaults. Start from `config/campaign.example.json` and replace every command
adapter only in the experiment's private setup.

The analyzers likewise require explicit input and output paths. Their outputs
are descriptive evidence; claim eligibility remains a property of the owning
experiment and its run contract. The fixed trial and comparison rules are in
`protocol.md`.

Run all helper tests from this directory:

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=../cws-control-daemon \
  python3 -B -m unittest discover -s tests -v
```

Historical site configurations, operator identities, device endpoints, reset
shells, service units, datasets, and previous result bundles are intentionally
not maintained here. Legacy single-campaign analyzers are not part of this
portable helper; use the maintained generic analyzers with an explicit input
and output path.
