"""Claimed welcome gifts survive progression resets without a second award."""
import copy
import json
import os
import tempfile
import unittest
from unittest import mock

from api import main, member_access, member_progress, progress_reset


class VerificationPackRestorationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.env = mock.patch.dict(os.environ, {
            "ALCOVE_STATE_DB_PATH": self.tmp.name + "/state.db",
            "ALCOVE_VERIFIED_MEMBER_RECOVERIES": "[]",
        })
        self.env.start()
        self.addCleanup(self.env.stop)
        for target, value in (("miniapp_verifications", []), ("synced_alcove_users", [])):
            patcher = mock.patch.object(main, target, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.catalog = {
            "verification_packs_active": False,
            "verification_packs": {
                key: {"id": key, "items": [
                    {"type": "skin", "id": key + "_gift", "name": key.title() + " skin",
                     "image": "assets/" + key + ".png", "layout": "banner"},
                ]} for key in ("biker", "dancer", "daddy", "pup")
            },
        }
        catalog = mock.patch.object(main, "load_reward_catalog", side_effect=lambda: copy.deepcopy(self.catalog))
        catalog.start()
        self.addCleanup(catalog.stop)
        joined = mock.patch.object(member_progress, "resolve_member_since", return_value="2026-09-01T00:00:00+00:00")
        joined.start()
        self.addCleanup(joined.stop)
        offline = mock.patch("urllib.request.urlopen", side_effect=AssertionError("Tests must stay offline"))
        offline.start()
        self.addCleanup(offline.stop)

    def retain(self, uid="42", pack="pup", claimed=True, authoritative=True):
        member_access.remember_member(
            {"user_id": uid, "username": "member", "fox_form": pack, "verified_at": "2026-10-01T00:00:00Z"},
            source="owner_confirmed_previous_verification" if authoritative else "bot_sync", claimed=claimed,
        )

    def seed(self, uid="42", profile=None):
        profile = member_progress.normalize_profile(profile or {})
        with member_progress._conn() as con:
            con.execute("INSERT OR REPLACE INTO member_progress VALUES (?, ?, ?)", (uid, json.dumps(profile), "old"))
        return profile

    def stored(self, uid="42"):
        with member_progress._conn() as con:
            row = con.execute("SELECT payload_json FROM member_progress WHERE user_id=?", (uid,)).fetchone()
        return json.loads(row[0]) if row else None

    def test_load_after_reset_restores_only_selected_gift_and_persists(self):
        for index, key in enumerate(("biker", "dancer", "daddy", "pup"), 1):
            uid = str(index)
            self.retain(uid, key)
            original = member_progress.normalize_profile({"level": 20, "exp": 99, "owned": {"feedSkins": ["old-level-item"]}})
            fresh = progress_reset.fresh_profile(original, "2026-11-13T00:00:00Z", "reset-test")
            self.seed(uid, fresh)
            repaired = member_progress.load_profile(uid)
            self.assertIn(key + "_gift", repaired["owned"]["feedSkins"])
            self.assertNotIn("old-level-item", repaired["owned"]["feedSkins"])
            self.assertEqual(set(repaired["owned"]["feedSkins"]) & {p + "_gift" for p in ("biker", "dancer", "daddy", "pup")}, {key + "_gift"})
            self.assertEqual((repaired["level"], repaired["exp"]), (1, 0))
            self.assertEqual(repaired["claimReceipts"], fresh["claimReceipts"])
            self.assertEqual(repaired["newUnlocks"], fresh["newUnlocks"])
            self.assertEqual(repaired["stats"], fresh["stats"])
            self.assertEqual(repaired["ownedMeta"]["skin:" + key + "_gift"]["image"], "assets/" + key + ".png")
            self.assertIn(key + "_gift", self.stored(uid)["owned"]["feedSkins"])
            self.assertEqual(member_progress.load_profile(uid), repaired)

    def test_save_replace_repairs_without_changing_progress_receipts_or_unrelated_rewards(self):
        self.retain()
        payload = {
            "level": 3, "exp": 17, "expView": {"level": 2, "exp": 8},
            "claimReceipts": {"verification_pack": {"at": "original"}, "level_2": True},
            "expHistory": [{"id": "old", "exp": 25, "at": "original"}],
            "claimEventIds": {"already": True}, "approvalClaims": {"pulse:1": True},
            "newUnlocks": ["sticker:unrelated"], "stats": {"cosmetics": 8},
            "owned": {"feedStickers": ["unrelated"]},
            "pendingRewards": [{"sourceKey": "verification_pack"}, {"sourceKey": "other-pack", "id": "keep"}],
        }
        normalized = member_progress.normalize_profile(payload)
        repaired = member_progress.save_profile("42", payload, replace=True)
        self.assertIn("pup_gift", repaired["owned"]["feedSkins"])
        self.assertEqual(repaired["owned"]["feedStickers"], ["unrelated"])
        for field in ("level", "exp", "expView", "claimReceipts", "claimEventIds", "approvalClaims", "expHistory", "newUnlocks", "stats"):
            self.assertEqual(repaired[field], normalized[field], field)
        self.assertEqual(repaired["pendingRewards"], [{"sourceKey": "other-pack", "id": "keep"}])

    def test_read_repair_cleans_only_stale_verification_pending_without_new_receipt(self):
        self.retain()
        self.seed(profile={"pendingRewards": [{"id": "verification_pack"}, {"sourceKey": "other", "id": "keep"}]})
        repaired = member_progress.load_profile("42")
        self.assertEqual(repaired["pendingRewards"], [{"sourceKey": "other", "id": "keep"}])
        self.assertEqual(repaired["claimReceipts"], {})
        self.assertEqual(repaired["newUnlocks"], [])
        self.assertEqual((repaired["level"], repaired["exp"]), (1, 0))

    def test_preserves_existing_art_and_fills_blanks_for_all_catalog_types(self):
        self.retain()
        self.catalog["verification_packs"]["pup"]["items"] += [
            {"type": "color", "id": "warm", "swatch": "#aabbcc"},
            {"type": "sticker", "id": "fox", "name": "Fox", "image": "fox.png"},
            {"type": "backdrop", "id": "room", "image": "room.png"},
            {"type": "title", "id": "friend", "name": "Friend"},
            {"type": "exp", "id": "bad", "amount": 500},
        ]
        self.seed(profile={"owned": {"feedSkins": ["pup_gift"]}, "ownedMeta": {"skin:pup_gift": {
            "name": "Original name", "image": "original.png", "obtainedAt": "original-time", "custom": "preserve",
        }}})
        repaired = member_progress.load_profile("42")
        meta = repaired["ownedMeta"]["skin:pup_gift"]
        self.assertEqual(meta["image"], "original.png")
        self.assertEqual(meta["name"], "Original name")
        self.assertEqual(meta["obtainedAt"], "original-time")
        self.assertEqual(meta["custom"], "preserve")
        self.assertEqual(meta["layout"], "banner")
        for field, item in (("feedColors", "warm"), ("feedStickers", "fox"), ("feedBackdrops", "room"), ("titles", "friend")):
            self.assertIn(item, repaired["owned"][field])
        self.assertEqual(repaired["ownedMeta"]["color:warm"]["swatch"], "#aabbcc")
        self.assertNotIn("exp:bad", repaired["ownedMeta"])
        self.assertEqual(repaired["exp"], 0)

    def test_no_entitlement_from_client_receipt_unclaimed_unknown_or_untrusted_registry(self):
        for uid, pack, claimed, authoritative in (("1", "pup", False, True), ("2", "unknown", True, True), ("3", "pup", True, False)):
            self.retain(uid, pack, claimed, authoritative)
            self.seed(uid)
            self.assertNotIn("pup_gift", member_progress.load_profile(uid)["owned"]["feedSkins"])
        repaired = member_progress.save_profile("999", {"claimReceipts": {"verification_pack": True}})
        self.assertNotIn("pup_gift", repaired["owned"]["feedSkins"])

    def test_revoked_registry_does_not_restore_gifts(self):
        self.retain()
        member_access.revoke_member("42", "test", revoked_at="2026-11-13T00:00:00Z")
        self.seed()
        self.assertNotIn("pup_gift", member_progress.load_profile("42")["owned"]["feedSkins"])

    def test_missing_profile_restores_claimed_gift_without_new_pack_eligibility(self):
        self.retain()
        repaired = member_progress.load_profile("42")
        self.assertIsNotNone(repaired)
        self.assertIn("pup_gift", repaired["owned"]["feedSkins"])
        self.assertEqual((repaired["level"], repaired["exp"]), (1, 0))
        self.assertEqual(repaired["claimReceipts"], {})
        self.assertFalse(main.verification_pack_eligibility(42)["eligible"])
        self.assertTrue(main.verification_pack_eligibility(42)["already_claimed"])

    def test_claimed_eligibility_uses_retained_gift_not_an_incomplete_new_choice(self):
        self.retain(pack="pup")
        main.miniapp_verifications.append({"id": 5, "user_id": 42, "selected_pack": "dancer", "status": "pending"})
        result = main.verification_pack_eligibility(42)
        self.assertEqual(result["pack_key"], "pup")
        self.assertEqual(result["pack"]["id"], "pup")
        self.assertFalse(result["eligible"])

    def test_missing_skin_layout_uses_existing_catalog_display_defaults(self):
        self.retain()
        self.catalog["verification_packs"]["pup"]["items"] = [
            {"type": "skin", "id": "foxlove"}, {"type": "skin", "id": "custom_stamp"},
        ]
        repaired = member_progress.save_profile("42", {})
        self.assertEqual(repaired["ownedMeta"]["skin:foxlove"]["layout"], "banner")
        self.assertEqual(repaired["ownedMeta"]["skin:custom_stamp"]["layout"], "stamp")

    def test_stale_upload_after_admin_reset_cannot_bypass_repair_or_restore_levels(self):
        self.retain()
        self.seed(profile=progress_reset.fresh_profile({}, "2026-11-13T00:00:00Z", "admin-reset"))
        with progress_reset._conn() as con:
            con.execute("INSERT INTO progression_member_resets VALUES (?, ?, ?, ?)",
                        ("42", "batch", "2026-11-13T00:00:00Z", "admin-reset"))
            con.execute("INSERT INTO progression_reset_approvals VALUES (?, ?)", ("batch", "{}"))
        result = member_progress.save_profile("42", {"level": 50, "exp": 1000}, replace=True)
        self.assertEqual((result["level"], result["exp"]), (1, 0))
        self.assertIn("pup_gift", result["owned"]["feedSkins"])
        self.assertIn("pup_gift", self.stored()["owned"]["feedSkins"])


if __name__ == "__main__":
    unittest.main()
