from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from run_hardware_reset_campaign import HardwareResetCampaign
from run_usb_power_campaign import UsbPowerCampaign


class SpecializedCampaignTests(unittest.TestCase):
    def campaign_paths(self, root: Path) -> dict[str, Path]:
        return {
            "ledger": root / "ledger.ndjson",
            "decisions": root / "decisions.ndjson",
            "actuation_inhibit_file": root / "inhibit",
        }

    def test_hardware_reset_trials_cover_every_source(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            campaign = HardwareResetCampaign(
                {
                    "schema_version": 1,
                    "seed": 11,
                    "safe_rate_hz": 40,
                    "repetitions_per_node": 2,
                    "nodes": {"source-a": {}, "source-b": {}},
                },
                **self.campaign_paths(root),
            )
            trials = campaign.build_trials()
        self.assertEqual(len(trials), 4)
        self.assertEqual({trial["source_id"] for trial in trials}, {"source-a", "source-b"})
        self.assertTrue(all(trial["fault_type"] == "esp-hardware-reset" for trial in trials))

    def test_power_loss_trials_retain_configured_duration(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            campaign = UsbPowerCampaign(
                {
                    "schema_version": 1,
                    "seed": 12,
                    "safe_rate_hz": 40,
                    "repetitions_per_node": 1,
                    "fault_seconds": 7.5,
                    "nodes": {"source-a": {}, "source-b": {}},
                },
                **self.campaign_paths(root),
            )
            trials = campaign.build_trials()
        self.assertEqual(len(trials), 2)
        self.assertEqual({trial["fault_seconds"] for trial in trials}, {7.5})
        self.assertTrue(all(trial["fault_type"] == "usb-power-cycle" for trial in trials))


if __name__ == "__main__":
    unittest.main()
