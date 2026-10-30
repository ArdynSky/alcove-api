import json
import os
import tempfile
import unittest
from unittest import mock

from fastapi.testclient import TestClient
from api import main


class VerifiedMemberAccessTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.env = mock.patch.dict(os.environ, {"ALCOVE_STATE_DB_PATH": self.tmp.name + "/state.db", "ALCOVE_VERIFIED_MEMBER_RECOVERIES": "[]"})
        self.env.start()
        self.patches = [mock.patch.object(main, "synced_alcove_users", []), mock.patch.object(main, "miniapp_verifications", []), mock.patch.object(main, "fox_db_rows", return_value=[]), mock.patch.object(main, "load_tester_usernames", return_value=[]), mock.patch.object(main, "verify_telegram_init_data", side_effect=self.verify)]
        for p in self.patches: p.start()
        self.client = TestClient(main.app)

    def tearDown(self):
        for p in reversed(self.patches): p.stop()
        self.env.stop()
        self.tmp.cleanup()

    def verify(self, value):
        if value != "valid-signed-user":
            raise main.HTTPException(403, "Invalid Telegram signature")
        return {"id": 919308569, "username": "current_name"}

    def access(self):
        return self.client.post("/api/membership/access", headers={"X-Telegram-Init-Data": "valid-signed-user"})

    def recover(self, claimed=True):
        os.environ["ALCOVE_VERIFIED_MEMBER_RECOVERIES"] = json.dumps([{"user_id": 919308569, "pack_key": "dancer", "pack_claimed": claimed, "reason": "Owner confirmed previous completed verification and Dancer pack claim"}])

    def test_missing_signature_and_unverified_identity_are_denied(self):
        self.assertEqual(self.client.post("/api/membership/access").status_code, 401)
        self.assertEqual(self.access().status_code, 403)
        response = self.client.get("/api/pulse-status?user_id=919308569", headers={"X-Telegram-Init-Data": "valid-signed-user"})
        self.assertEqual(response.status_code, 403)

    def test_owner_confirmed_recovery_is_durable_and_prevents_duplicate_pack(self):
        self.recover()
        self.assertEqual(self.access().status_code, 200)
        os.environ["ALCOVE_VERIFIED_MEMBER_RECOVERIES"] = "[]"
        self.assertEqual(self.access().json()["access_reason"], "verified")
        data = main.verification_pack_eligibility(919308569)
        self.assertEqual(data["pack_key"], "dancer")
        self.assertTrue(data["already_claimed"])

    def test_completed_verification_survives_missing_sync_but_selection_does_not_verify(self):
        main.miniapp_verifications.append({"user_id": 919308569, "status": "pending", "selected_pack": "dancer"})
        self.assertEqual(self.access().status_code, 403)
        main.miniapp_verifications[0].update(status="completed", completed_at="2026-07-01T00:00:00Z")
        self.assertEqual(self.access().status_code, 200)
        main.miniapp_verifications.clear()
        self.assertEqual(self.access().status_code, 200)

    def test_unsigned_cached_username_does_not_authorize_and_other_user_id_is_blocked(self):
        self.recover()
        self.assertEqual(self.client.get("/api/pulse-status?user_id=919308569").status_code, 401)
        response = self.client.get("/api/pulse-status?user_id=123", headers={"X-Telegram-Init-Data": "valid-signed-user"})
        self.assertEqual(response.status_code, 403)

    def test_live_signed_tester_access_is_an_explicit_exception(self):
        with mock.patch.object(main, "load_tester_usernames", return_value=["current_name"]):
            self.assertEqual(self.access().json()["access_reason"], "tester")
        self.assertEqual(self.access().status_code, 403)

    def test_signature_is_required_for_owners_too(self):
        with mock.patch.object(main, "verify_telegram_init_data", return_value={"id": 5737853594}):
            self.assertEqual(self.client.post("/api/membership/access").status_code, 401)
            self.assertEqual(self.access().json()["access_reason"], "owner")

    def test_numeric_pack_lookup_cannot_follow_recycled_username(self):
        main.miniapp_verifications.append({"user_id": 123, "username": "current_name", "status": "completed", "selected_pack": "biker"})
        with mock.patch.object(main, "verification_pack_choices", {"name:current_name": {"pack_key": "pup"}}), mock.patch.object(main, "verification_flow_pack_index", return_value={"by_user": {}, "by_name": {"current_name": "daddy"}}):
            self.assertIsNone(main.find_user_verification_pack_key(919308569, "current_name"))
            self.assertEqual(main.find_user_verification_pack_key(None, "current_name"), "biker")

    def test_recovery_upgrades_existing_registry_claim_without_rewriting_dates(self):
        from api.member_access import remember_member, find_retained_member
        remember_member({"user_id": 919308569, "verified_at": "2026-07-01T00:00:00Z", "fox_form": "dancer"})
        self.recover()
        member = find_retained_member(919308569)
        self.assertTrue(member["verification_pack_claimed"])
        self.assertEqual(member["verified_at"], "2026-07-01T00:00:00Z")

    def test_registry_imports_existing_profile_claims_without_issuing_rewards(self):
        from api import member_progress
        from api.member_access import remember_member, find_retained_member
        member_progress.save_profile("919308569", {"claimReceipts": {"verification_pack": {"claimedAt": "2026-07-01"}}})
        remember_member({"user_id": 919308569, "verified_at": "2026-07-01", "fox_form": "dancer"})
        self.assertTrue(find_retained_member(919308569)["verification_pack_claimed"])
