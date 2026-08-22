from api import debate_audience


def test_thoughts_are_open_through_debate_and_close_for_voting():
    open_stages = {
        "pooling", "registration_closed", "selected", "intro_for", "speaker_for",
        "holding_against", "intro_against", "speaker_against", "holding_vote",
    }
    for stage in open_stages:
        assert debate_audience._submissions_open({"status": stage}) is True
    assert debate_audience._submissions_open({"status": "voting"}) is False
    assert debate_audience._submissions_open({"status": "results"}) is False


def test_new_debate_session_has_a_fresh_member_submission(monkeypatch, tmp_path):
    monkeypatch.setattr(debate_audience, "DB_PATH", tmp_path / "audience.sqlite3")
    state = {"session_id": "debate-one", "status": "pooling", "contestants": []}
    monkeypatch.setattr(debate_audience, "_load", lambda: state)
    first = debate_audience.AudienceThoughtPayload(
        user_id="member-1", display_name="Member", side="FOR", reason="First debate thought"
    )
    debate_audience.audience_submit(first)
    assert debate_audience.audience_state("member-1")["mine"]["reason"] == "First debate thought"
    state["session_id"] = "debate-two"
    fresh = debate_audience.audience_state("member-1")
    assert fresh["mine"] is None
    assert fresh["count"] == 0
    assert fresh["submissions_open"] is True
