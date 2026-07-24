import os
import tempfile
import unittest

_temp_dir = tempfile.TemporaryDirectory()
os.environ.setdefault("ALCOVE_STATE_DB_PATH", os.path.join(_temp_dir.name, "state.db"))
os.environ.setdefault("ALCOVE_RUNTIME_STATE_PATH", os.path.join(_temp_dir.name, "runtime.json"))
os.environ.setdefault("FEATURE_FLAGS_PATH", os.path.join(_temp_dir.name, "feature_flags.json"))
os.environ.setdefault("PULSE_SETTINGS_PATH", os.path.join(_temp_dir.name, "pulse_settings.json"))
os.environ.setdefault("SAFETY_SETTINGS_PATH", os.path.join(_temp_dir.name, "safety_settings.json"))
os.environ.setdefault("VERIFY_FLOW_LOG_PATH", os.path.join(_temp_dir.name, "verification_flow_events.jsonl"))

from api import main


class WheelReserveTests(unittest.TestCase):
    def setUp(self):
        self.original_entries = list(main.wheel_entries)
        self.original_archived = list(main.archived_wheel_entries)
        self.original_state = dict(main.state)
        self.original_winner = main.current_winner
        main.wheel_entries[:] = []
        main.archived_wheel_entries[:] = []
        main.current_winner = None
        main.state["current_round"] = 3
        main.state["round_status"] = "locked"

    def tearDown(self):
        main.wheel_entries[:] = self.original_entries
        main.archived_wheel_entries[:] = self.original_archived
        main.state.clear()
        main.state.update(self.original_state)
        main.current_winner = self.original_winner

    def _entry(self, entry_id, status="approved", played=False, reserved=False):
        return {
            "id": entry_id,
            "round_id": 3,
            "played": played,
            "approval_status": status,
            "reserved": reserved,
            "data": {"display_name": f"User{entry_id}", "video_title": f"Vid{entry_id}"},
        }

    def test_end_round_moves_unplayed_to_reserve(self):
        main.wheel_entries.extend([
            self._entry(1, "approved"),
            self._entry(2, "pending"),
            self._entry(3, "approved", played=True),
            self._entry(4, "rejected"),
        ])
        result = main.end_round()
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["reserved_count"], 2)
        reserved_ids = {e["id"] for e in main.wheel_entries if e.get("reserved")}
        self.assertEqual(reserved_ids, {1, 2})
        self.assertFalse(next(e for e in main.wheel_entries if e["id"] == 3).get("reserved"))
        self.assertEqual(main.state["current_round"], 4)

    def test_resubmit_and_delete_reserve_entry(self):
        main.wheel_entries.append(self._entry(9, "pending", reserved=True))
        main.wheel_entries[-1]["round_id"] = 2
        result = main.resubmit_wheel_entry(9)
        self.assertEqual(result["status"], "ok")
        entry = main.wheel_entries[0]
        self.assertFalse(entry.get("reserved"))
        self.assertEqual(entry["round_id"], 3)
        self.assertEqual(entry["approval_status"], "approved")
        self.assertIn(entry, main.get_wheel_spin_pool(3))

        delete = main.delete_wheel_entry(9)
        self.assertEqual(delete["status"], "ok")
        self.assertEqual(main.wheel_entries, [])
        self.assertEqual(main.archived_wheel_entries, [])


if __name__ == "__main__":
    unittest.main()
