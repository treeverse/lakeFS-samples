from common.redaction import REDACTED, redact, redact_text


def test_redacts_sensitive_keys():
    out = redact(
        {
            "secret_access_key": "supersecret",
            "LAKEFS_SECRET_ACCESS_KEY": "s",
            "Authorization": "Basic abc",
            "path": "corpus/x.json",
            "nested": {"password": "p", "ok": "v"},
        }
    )
    assert out["secret_access_key"] == REDACTED
    assert out["LAKEFS_SECRET_ACCESS_KEY"] == REDACTED
    assert out["Authorization"] == REDACTED
    assert out["path"] == "corpus/x.json"
    assert out["nested"]["password"] == REDACTED
    assert out["nested"]["ok"] == "v"


def test_redacts_inline_auth_headers():
    assert REDACTED in redact_text("Authorization: Basic dXNlcjpwYXNz")
    assert REDACTED in redact_text("using Bearer abcdefghijklmnop token")
    assert "hello" in redact_text("hello world")


def test_redacts_in_lists():
    out = redact([{"token": "t"}, {"clean": "c"}])
    assert out[0]["token"] == REDACTED
    assert out[1]["clean"] == "c"
