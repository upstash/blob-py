import re
from datetime import datetime, timedelta, timezone

import pytest
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.credentials import Credentials

from upstash_blob import BlobError, format_bytes, unique_path
from upstash_blob._core import attachment_disposition, error_from_body, safe_content_type
from upstash_blob._keys import encode_key, meta_headers, presignable_key, uri_encode
from upstash_blob._multipart import MULTIPART_THRESHOLD, SINGLE_PUT_MAX, part_count, part_size_for, wants_multipart
from upstash_blob._sigv4 import sign_headers
from upstash_blob._sniff import check_content_type, expand_content_types, sniff
from upstash_blob._token import decode_token, encode_token
from upstash_blob._units import cache_control, over_limit, parse_duration, parse_size

PNG = bytes([0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0, 0, 0, 0x0D, 0x49, 0x48, 0x44, 0x52, 0, 0, 0, 1])


class TestUnits:
    def test_sizes_are_decimal(self) -> None:
        assert parse_size("2mb") == 2_000_000
        assert parse_size(" 1.5 KB ") == 1500
        assert parse_size(42) == 42
        for bad in ("2 parsecs", "mb", "-1", ""):
            with pytest.raises(ValueError):
                parse_size(bad)
        with pytest.raises(TypeError):
            parse_size(True)

    def test_durations_are_seconds(self) -> None:
        assert parse_duration("15m") == 900
        assert parse_duration(90) == 90
        assert parse_duration(timedelta(hours=1)) == 3600
        assert parse_duration("250ms") == 0.25
        with pytest.raises(ValueError):
            parse_duration("1 fortnight")

    def test_format_bytes(self) -> None:
        assert format_bytes(2_000_000) == "2 MB"
        assert format_bytes(1_500_000) == "1.5 MB"
        assert format_bytes(999_960) == "1.0 MB"
        assert format_bytes(12_345) == "12 KB"
        assert format_bytes(0) == "0 B"

    def test_cache_control(self) -> None:
        assert cache_control(None) == "public, max-age=3600"
        assert cache_control("immutable") == "public, max-age=31536000, immutable"
        assert cache_control("revalidate", "private") == "private, max-age=0, must-revalidate"
        assert cache_control("no-store") == "no-store"
        assert cache_control("15m") == "public, max-age=900"
        assert cache_control(0) == "public, max-age=0"
        assert cache_control(" s-maxage=60, public ") == "s-maxage=60, public"


class TestKeys:
    def test_encode_key_refuses_traversal(self) -> None:
        assert encode_key("a b/ü+x.png") == "a%20b/%C3%BC%2Bx.png"
        for bad in ("../x", "a/./b", "a/.."):
            with pytest.raises(ValueError):
                encode_key(bad)
        with pytest.raises(ValueError):
            encode_key("")

    def test_presignable_key(self) -> None:
        for bad in ("dir/", "a//b", "/a", "a\nb"):
            with pytest.raises(BlobError) as e:
                presignable_key(bad)
            assert e.value.code == "invalid_input"

    def test_metadata_is_printable_ascii(self) -> None:
        assert meta_headers({"owner": "u1"}) == {"x-amz-meta-owner": "u1"}
        for bad in ({"note": "café"}, {"note": "a\r\nb"}, {"bad key": "x"}, {"k": 1}, {"recordId": "x"}):
            with pytest.raises(BlobError) as e:
                meta_headers(bad)  # type: ignore[arg-type]
            assert e.value.code == "invalid_input"

    def test_uri_encode(self) -> None:
        assert uri_encode("a b*'()!~") == "a%20b%2A%27%28%29%21~"


class TestToken:
    def test_round_trip(self) -> None:
        t = decode_token(encode_token("id", "pw", "b0123456789a", 3))
        assert (t.bucket_id, t.password, t.hash_for_domain, t.flags) == ("id", "pw", "b0123456789a", 3)

    def test_refuses_tampering(self) -> None:
        token = encode_token("id", "pw", "h")
        for bad in (token + "AA", "!!!", "AQ", encode_token("id", "pw", "h")[:-2]):
            with pytest.raises(ValueError):
                decode_token(bad)


class TestSigV4:
    def test_matches_botocore(self, monkeypatch: pytest.MonkeyPatch) -> None:
        when = datetime(2026, 8, 24, 12, 34, 56, tzinfo=timezone.utc)
        host = "acct.r2.cloudflarestorage.com"
        path = "/bucket-id/some%20key/x.png"
        query = {"partNumber": "3", "uploadId": "abc/def"}
        headers = {"content-type": "image/png", "content-length": "12", "x-amz-meta-foo": "bar  baz"}

        ours = sign_headers(
            access_key_id="AKIAEXAMPLE",
            secret_access_key="secretsecretsecret",
            session_token="tok/en+123",
            region="auto",
            method="PUT",
            host=host,
            path=path,
            query=query,
            headers=headers,
            now=when,
        )

        request = AWSRequest(
            method="PUT",
            url=f"https://{host}{path}?partNumber=3&uploadId=abc%2Fdef",
            headers={**headers, "X-Amz-Content-SHA256": "UNSIGNED-PAYLOAD"},
        )
        auth = S3SigV4Auth(Credentials("AKIAEXAMPLE", "secretsecretsecret", "tok/en+123"), "s3", "auto")
        # Over https with payload signing off, botocore signs UNSIGNED-PAYLOAD too.
        request.context["client_config"] = Config(s3={"payload_signing_enabled": False})
        monkeypatch.setattr("botocore.auth.get_current_datetime", lambda: when.replace(tzinfo=None))
        auth.add_auth(request)

        assert ours["authorization"] == request.headers["Authorization"]
        assert "host" not in ours
        assert ours["x-amz-security-token"] == "tok/en+123"

    def test_no_session_token(self) -> None:
        out = sign_headers(
            access_key_id="A",
            secret_access_key="S",
            session_token=None,
            region="auto",
            method="GET",
            host="h",
            path="/b/k",
        )
        assert "x-amz-security-token" not in out
        assert "x-amz-security-token" not in out["authorization"]


class TestSniff:
    def test_signatures(self) -> None:
        assert sniff(PNG) == "image/png"
        assert sniff(PNG[:8]) is None
        assert sniff(b"\xff\xd8\xff\xe0") == "image/jpeg"
        assert sniff(b"RIFF\0\0\0\0WEBP") == "image/webp"
        assert sniff(b"%PDF-1.7") == "application/pdf"
        assert sniff(b"BM hello there, not a bitmap") is None
        assert sniff(b"<svg/>") is None

    def test_wildcards_and_patterns(self) -> None:
        images = expand_content_types(["image/*", "image/png", "IMAGE/JPG"])
        assert "image/png" in images and "image/svg+xml" not in images
        assert images.count("image/jpeg") == 1
        for bad in (["*/*"], ["text/*"], ["nonsense"], []):
            with pytest.raises(BlobError) as e:
                expand_content_types(bad)
            assert e.value.code == "invalid_content_type_pattern"

    def test_only_a_proven_lie_is_refused(self) -> None:
        allowed = expand_content_types(
            ["image/*", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"]
        )
        check_content_type("image/png", PNG, allowed)
        check_content_type("image/png", None, allowed)
        # A .docx really is a zip: agreement, not a lie.
        check_content_type(
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document", b"PK\x03\x04", allowed
        )
        with pytest.raises(BlobError) as e:
            check_content_type("image/png", b"PK\x03\x04", allowed)
        assert e.value.code == "content_type_not_allowed"
        with pytest.raises(BlobError):
            check_content_type("image/svg+xml", b"<svg/>", allowed)


class TestMultipart:
    def test_threshold(self) -> None:
        assert not wants_multipart(None, MULTIPART_THRESHOLD)
        assert wants_multipart(None, MULTIPART_THRESHOLD + 1)
        assert wants_multipart(True, 10)
        assert not wants_multipart(False, 10**9)
        assert wants_multipart("1mb", 1_000_001)
        with pytest.raises(BlobError) as e:
            wants_multipart(False, SINGLE_PUT_MAX + 1)
        assert e.value.code == "too_large"

    def test_part_size(self) -> None:
        assert part_size_for(17_000_000) == 5 * 1024 * 1024
        assert part_count(17_000_000, part_size_for(17_000_000)) == 4
        big = 10 * 1024**3
        assert part_count(big, part_size_for(big)) <= 250


class TestErrors:
    def test_messages_are_sentence_cased_unless_an_identifier(self) -> None:
        assert str(BlobError("not_found")) == "Not found"
        assert BlobError("content_type_not_allowed", "image/png is not allowed").message == "image/png is not allowed"
        assert str(BlobError("length_required")).startswith("Length required (pass size=")

    def test_to_dict(self) -> None:
        e = BlobError("partial_delete", "1 of 2", failed=["a"], reason="owner-only")
        assert e.to_dict() == {"code": "partial_delete", "message": "1 of 2", "status": 500, "failed": ["a"]}

    def test_error_from_body(self) -> None:
        assert error_from_body(404, "").code == "not_found"
        assert error_from_body(403, "<Error><Code>ExpiredToken</Code></Error>").code == "unauthorized"
        assert error_from_body(403, "<Error><Code>SignatureDoesNotMatch</Code></Error>").code == "signature_mismatch"
        e = error_from_body(500, "<Error><Code>InternalError</Code><Message>boom</Message></Error>")
        assert (e.code, e.status, e.message) == ("request_failed", 502, "Storage responded 500 InternalError")
        assert str(e.__cause__) == "boom" and "boom" not in str(e.to_dict())


class TestHeaders:
    def test_disposition_cannot_inject(self) -> None:
        d = attachment_disposition('a"; filename=evil.exe\r\nX-Bad: 1')
        assert "\r" not in d and "\n" not in d
        assert d.startswith('attachment; filename="a_ filename_evil.exe_X-Bad_ 1"')
        assert attachment_disposition("rapor ü.pdf").endswith("filename*=UTF-8''rapor%20%C3%BC.pdf")

    def test_content_type_must_be_a_media_type(self) -> None:
        assert safe_content_type('text/plain; charset="utf-8"') == 'text/plain; charset="utf-8"'
        for bad in ("text/plain\n", "text/plain\r\nx: y", "nope"):
            with pytest.raises(BlobError):
                safe_content_type(bad)


_SUFFIX = "[1-9A-HJ-NP-Za-km-z]{8}"


def _strip_suffix(path: str) -> str:
    return re.sub(rf"-{_SUFFIX}(?=\.[a-z0-9]{{1,8}}$|$)", "", path, flags=re.IGNORECASE)


class TestSizes:
    def test_binary_units(self) -> None:
        assert parse_size("32MiB") == 32 * 1024 * 1024
        assert parse_size("1kib") == 1024 and parse_size("2GiB") == 2 * 1024**3 and parse_size("1TiB") == 1024**4
        assert parse_size("32MB") == 32_000_000
        with pytest.raises(ValueError, match="kib, mib"):
            parse_size("3 bits")

    def test_over_limit_shows_bytes_when_both_sides_round_alike(self) -> None:
        assert over_limit(3_000_000, 2_000_000) == "3 MB, over the 2 MB limit"
        assert over_limit(33_554_433, 33_554_432) == "33,554,433 bytes, over the 33,554,432 byte limit"
        assert over_limit(2_000_001, 2_000_000) == "2,000,001 bytes, over the 2,000,000 byte limit"


class TestUniquePath:
    def test_keeps_the_path_as_given(self) -> None:
        path = unique_path("users/Alice_123/Q3 Report (final).pdf")
        assert re.fullmatch(rf"users/Alice_123/Q3 Report \(final\)-{_SUFFIX}\.pdf", path), path
        for name in [
            "café.pdf",
            "日本語.pdf",
            "a🙂b&c.pdf",
            "a\u200bb.png",
            "!!! ***",
            "  a.png  ",
            "A_" * 80 + ".png",
        ]:
            assert _strip_suffix(unique_path(f"u/{name}")) == f"u/{name}"
        for owner in ["caf\u00e9", "cafe\u0301"]:
            assert _strip_suffix(unique_path(f"users/{owner}/x.pdf")) == f"users/{owner}/x.pdf"

    def test_the_suffix_goes_before_the_final_extension(self) -> None:
        assert re.fullmatch(rf"REPORT-{_SUFFIX}\.PDF", unique_path("REPORT.PDF"))
        assert re.fullmatch(rf"x\.exe-{_SUFFIX}\.png", unique_path("x.exe.png"))
        assert re.fullmatch(rf"x\.superlongext-{_SUFFIX}", unique_path("x.superlongext"))
        assert re.fullmatch(rf"\.env-{_SUFFIX}", unique_path(".env"))
        assert re.fullmatch(rf"\.env-{_SUFFIX}\.local", unique_path(".env.local"))
        assert re.fullmatch(rf"a\.png/thumbs/file-{_SUFFIX}", unique_path("a.png/thumbs/"))
        assert re.fullmatch(rf"file-{_SUFFIX}", unique_path(""))

    def test_traversal_is_refused_where_the_key_is_used(self) -> None:
        for path in (unique_path("uploads/../x.png"), unique_path("a/./b.png"), unique_path("../x.png")):
            with pytest.raises(ValueError):
                encode_key(path)
        assert re.fullmatch(rf"a/\.\.-{_SUFFIX}", unique_path("a/.."))

    def test_suffixes_are_random_base58(self) -> None:
        suffixes = [unique_path("a")[2:] for _ in range(500)]
        assert len(set(suffixes)) == 500
        seen = set("".join(suffixes))
        assert len(seen) == 58 and not seen & set("0OIl")


class TestReviewRegressions:
    def test_a_backslash_url_is_not_r2(self) -> None:
        from upstash_blob._core import _r2_https

        assert _r2_https("https://acc.r2.cloudflarestorage.com/b/k")
        assert not _r2_https("https://evil.example\\@acc.r2.cloudflarestorage.com/b/k")
        assert not _r2_https("https://user@acc.r2.cloudflarestorage.com/b/k")

    def test_secrets_stay_out_of_repr(self) -> None:
        from upstash_blob._core import Credentials

        c = Credentials("AK", "SECRET", "SESSION", "https://a.r2.cloudflarestorage.com", "b", "auto", 0, None)
        assert "SECRET" not in repr(c) and "SESSION" not in repr(c)
        assert "pw-secret" not in repr(decode_token(encode_token("id", "pw-secret", "h")))

    def test_iso_dates(self) -> None:
        from upstash_blob._core import iso_date

        assert iso_date("2026-09-29T10:00:00.12Z") == datetime(2026, 9, 29, 10, 0, 0, 120000, timezone.utc)
        assert iso_date("2026-09-29T10:00:00.1234567Z") == datetime(2026, 9, 29, 10, 0, 0, 123456, timezone.utc)
        assert iso_date("2026-09-29T12:00:00+02:00") == datetime(2026, 9, 29, 10, 0, 0, tzinfo=timezone.utc)
        assert iso_date("nope") is None

    def test_blob_error_pickles(self) -> None:
        import copy
        import pickle

        e = BlobError("already_exists", "a exists", etag='"e"', size=3, reason="r")
        for back in (pickle.loads(pickle.dumps(e)), copy.copy(e)):
            assert (back.code, back.message, back.etag, back.size, back.reason) == (
                "already_exists",
                "A exists",
                '"e"',
                3,
                "r",
            )
        hinted = BlobError("length_required")
        assert pickle.loads(pickle.dumps(hinted)).message == hinted.message

    def test_an_infinite_expiry_is_unexpected(self) -> None:
        import httpx

        from upstash_blob._core import presigned_from

        for expires in (1e999, 1e300):
            res = httpx.Response(
                200, content=f'{{"url": "https://a.r2.cloudflarestorage.com/x", "expiresAt": {expires}}}'.encode()
            )
            with pytest.raises(BlobError) as e:
                presigned_from(res)
            assert e.value.code == "request_failed"

    def test_the_signed_host_is_the_one_httpx_sends(self) -> None:
        from upstash_blob._core import normalized_host

        assert normalized_host("https://ACC.r2.cloudflarestorage.com:443") == "acc.r2.cloudflarestorage.com"
        assert normalized_host("https://acc.r2.cloudflarestorage.com:8443") == "acc.r2.cloudflarestorage.com:8443"
