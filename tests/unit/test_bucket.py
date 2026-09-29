import io
import json
from pathlib import Path
from typing import Any, Iterator, Optional

import httpx
import pytest

from tests.fake import ENDPOINT, OTHER_TOKEN, TOKEN, Fake, error
from upstash_blob import AsyncBucket, BlobError, Bucket, MultipartUpload
from upstash_blob._core import reset_credential_caches

PNG = bytes([0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0, 0, 0, 0x0D, 0x49, 0x48, 0x44, 0x52, 0, 0, 0, 1])


def make(fake: Fake, token: str = TOKEN, **kwargs: Any) -> Bucket:
    return Bucket(token, http_client=httpx.Client(transport=httpx.MockTransport(fake.handler)), **kwargs)


def code_of(fn: Any, *args: Any, **kwargs: Any) -> str:
    with pytest.raises(BlobError) as e:
        fn(*args, **kwargs)
    return e.value.code


class TestCredentials:
    def test_cached_by_token_across_buckets(self, fake: Fake) -> None:
        make(fake).exists("a")
        make(fake).exists("b")
        assert fake.mints == 1
        make(fake, OTHER_TOKEN).exists("c")
        assert fake.mints == 2

    def test_a_long_retry_after_is_an_error_not_a_stall(self, fake: Fake, bucket: Bucket) -> None:
        fake.mint_response = lambda: httpx.Response(429, headers={"retry-after": "45"})
        with pytest.raises(BlobError) as e:
            bucket.exists("a")
        assert (e.value.code, e.value.status, e.value.retry_after) == ("mint_backoff", 429, 45)
        assert fake.mints == 1

    def test_a_short_retry_after_is_waited_out(self, fake: Fake, bucket: Bucket) -> None:
        answers = [httpx.Response(503, headers={"retry-after": "1"}), httpx.Response(200, json=fake.creds())]
        fake.mint_response = lambda: answers.pop(0)
        assert bucket.exists("a") is False
        assert fake.mints == 2

    def test_rejected_token(self, fake: Fake, bucket: Bucket) -> None:
        fake.mint_response = lambda: httpx.Response(401)
        assert code_of(bucket.exists, "a") == "unauthorized"

    def test_an_endpoint_that_is_not_r2_is_refused(self, fake: Fake, bucket: Bucket) -> None:
        fake.mint_response = lambda: httpx.Response(200, json={**fake.creds(), "endpoint": "https://evil.example"})
        assert code_of(bucket.exists, "a") == "request_failed"

    def test_telemetry(self, fake: Fake, monkeypatch: pytest.MonkeyPatch) -> None:
        make(fake).exists("a")
        assert fake.calls[0].headers["upstash-telemetry-sdk"].startswith("upstash-blob-py@v")
        assert fake.calls[0].headers["authorization"] == f"Bearer {TOKEN}"
        reset_credential_caches()
        fake.calls.clear()
        make(fake, enable_telemetry=False).exists("a")
        assert "upstash-telemetry-sdk" not in fake.calls[0].headers
        reset_credential_caches()
        fake.calls.clear()
        monkeypatch.setenv("UPSTASH_DISABLE_TELEMETRY", "1")
        make(fake).exists("a")
        assert "upstash-telemetry-sdk" not in fake.calls[0].headers

    def test_from_env(self, fake: Fake, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("UPSTASH_BLOB_TOKEN", raising=False)
        with pytest.raises(TypeError):
            Bucket.from_env()
        monkeypatch.setenv("MEDIA_TOKEN", TOKEN)
        b = Bucket.from_env(
            "MEDIA_TOKEN", cache="1m", http_client=httpx.Client(transport=httpx.MockTransport(fake.handler))
        )
        assert b.signed_upload_url("f.txt").headers["cache-control"] == "public, max-age=60"


class TestRetries:
    def test_idempotent_verbs_retry_on_503_and_429(self, fake: Fake, bucket: Bucket) -> None:
        answers = [error(503, "Busy"), httpx.Response(429, headers={"retry-after": "0"})]
        fake.intercept = lambda r: answers.pop(0) if answers else None
        assert bucket.exists("a") is False
        assert len(fake.r2_calls()) == 3

    def test_gives_up_after_three_and_never_retries_a_post(self, fake: Fake, bucket: Bucket) -> None:
        fake.intercept = lambda r: error(500, "InternalError")
        assert code_of(bucket.exists, "a") == "request_failed"
        assert len(fake.r2_calls()) == 3
        fake.calls.clear()
        assert code_of(bucket.delete, ["a", "b"]) == "request_failed"
        assert len(fake.r2_calls()) == 1

    def test_a_dropped_connection_is_retried_then_wrapped(self, fake: Fake, bucket: Bucket) -> None:
        def drop(r: httpx.Request) -> Optional[httpx.Response]:
            raise httpx.ConnectError("boom", request=r)

        fake.intercept = drop
        with pytest.raises(BlobError) as e:
            bucket.exists("a")
        assert e.value.code == "request_failed"
        assert isinstance(e.value.__cause__, httpx.ConnectError)

    def test_an_expired_credential_is_reminted_once(self, fake: Fake, bucket: Bucket) -> None:
        fake.objects.clear()
        bucket.put("a.txt", "body")
        answers = [error(403, "ExpiredToken")]
        fake.intercept = lambda r: answers.pop(0) if answers else None
        assert bucket.get("a.txt").text() == "body"
        assert fake.mints == 2
        fake.intercept = lambda r: error(403, "ExpiredToken")
        fake.calls.clear()
        assert code_of(bucket.get, "a.txt") == "unauthorized"
        assert len(fake.r2_calls()) == 2

    def test_a_plain_403_is_a_signature_mismatch(self, fake: Fake, bucket: Bucket) -> None:
        fake.intercept = lambda r: error(403, "SignatureDoesNotMatch")
        assert code_of(bucket.get, "a") == "signature_mismatch"
        assert len(fake.r2_calls()) == 1


class TestPut:
    def test_bytes_then_read_back(self, fake: Fake, bucket: Bucket) -> None:
        blob = bucket.put("dir/a b.txt", "hello", content_type="text/plain", metadata={"Owner": "u1"})
        assert (blob.size, blob.content_type) == (5, "text/plain")
        assert blob.url == "https://b0123456789a.blob.upstash.io/dir/a%20b.txt"
        assert blob.versioned_url == f"{blob.url}?v=%22{blob.etag.strip(chr(34))}%22"
        put = fake.r2_calls()[-1]
        assert put.headers["cache-control"] == "public, max-age=3600"
        assert put.headers["content-length"] == "5"
        got = bucket.get("dir/a b.txt")
        assert (got.content, got.content_type, got.metadata, got.etag) == (
            b"hello",
            "text/plain",
            {"owner": "u1"},
            blob.etag,
        )
        info = bucket.info("dir/a b.txt")
        assert (info.size, info.metadata) == (5, {"owner": "u1"})
        assert bucket.exists("dir/a b.txt") and not bucket.exists("nope")
        assert code_of(bucket.get, "nope") == "not_found"
        assert code_of(bucket.info, "nope") == "not_found"

    def test_a_file_carries_its_size_and_type(self, fake: Fake, bucket: Bucket, tmp_path: Path) -> None:
        f = tmp_path / "photo.png"
        f.write_bytes(PNG)
        assert bucket.put("p.png", f).content_type == "image/png"
        with open(f, "rb") as handle:
            blob = bucket.put("q.png", handle, content_types=["image/*"])
        assert (blob.size, blob.content_type) == (len(PNG), "image/png")
        assert fake.objects["q.png"].data == PNG

    def test_sniffing_refuses_a_lie(self, fake: Fake, bucket: Bucket) -> None:
        assert code_of(bucket.put, "x.png", b"PK\x03\x04", content_type="image/png", content_types=["image/png"]) == (
            "content_type_not_allowed"
        )
        assert "x.png" not in fake.objects
        assert code_of(bucket.put, "x", "x", content_types=[]) == "invalid_content_type_pattern"

    def test_max_size_up_front_and_while_streaming(self, fake: Fake, bucket: Bucket) -> None:
        assert code_of(bucket.put, "big", b"x" * 3000, max_size="2kb") == "too_large"

        def lying() -> Iterator[bytes]:
            yield b"x" * 1500
            yield b"x" * 1500

        # A declared size is checked against max_size up front, and the stream against the declared size.
        assert code_of(bucket.put, "lying", lying(), size=3000, max_size="2kb") == "too_large"
        assert code_of(bucket.put, "lying", lying(), size=1000, max_size="2kb") == "invalid_input"
        assert "lying" not in fake.objects

    def test_unknown_length_buffers_under_max_size_or_is_refused(self, fake: Fake, bucket: Bucket) -> None:
        chunks = [b"a" * 3000, b"b" * 2000]
        assert bucket.put("buffered", iter(chunks), max_size="10kb").size == 5000
        assert fake.objects["buffered"].data == b"".join(chunks)
        assert bucket.put("sized", iter(chunks), size=5000).size == 5000
        assert code_of(bucket.put, "nolen", iter(chunks)) == "length_required"

    def test_a_text_mode_file_is_a_type_error(self, bucket: Bucket) -> None:
        with pytest.raises(TypeError):
            bucket.put("t", io.StringIO("text"), size=4)

    def test_conditional_writes(self, fake: Fake, bucket: Bucket) -> None:
        first = bucket.put("once", "one")
        with pytest.raises(BlobError) as e:
            bucket.put("once", "two", allow_overwrite=False)
        assert (e.value.code, e.value.status, e.value.etag, e.value.size) == ("already_exists", 409, first.etag, 3)
        assert code_of(bucket.put, "once", "three", if_unchanged='"deadbeef"') == "conflict"
        assert bucket.put("once", "four", if_unchanged=first.etag).size == 4

    def test_multipart_in_parts_and_nothing_left_behind(self, fake: Fake, bucket: Bucket) -> None:
        data = bytes(range(256)) * 50_000  # 12.8 MB
        blob = bucket.put("big.bin", io.BytesIO(data), multipart=True)
        assert blob.etag.endswith('-3"')
        assert fake.objects["big.bin"].data == data
        assert fake.uploads == {}
        parts = [c for c in fake.r2_calls() if "partNumber" in str(c.url)]
        assert [len(c.content) for c in parts] == [5 * 1024 * 1024, 5 * 1024 * 1024, len(data) - 10 * 1024 * 1024]

    def test_multipart_from_an_iterator_with_a_declared_size(self, fake: Fake, bucket: Bucket) -> None:
        chunks = [b"z" * 1_000_000] * 12
        blob = bucket.put("gen.bin", iter(chunks), size=12_000_000, multipart="1mb")
        assert blob.size == 12_000_000 and fake.objects["gen.bin"].data == b"".join(chunks)

    def test_a_failed_part_aborts_the_upload(self, fake: Fake, bucket: Bucket) -> None:
        fake.intercept = lambda r: error(400, "InvalidPart") if "partNumber=2" in str(r.url) else None
        assert code_of(bucket.put, "fail.bin", b"x" * 11_000_000, multipart=True) == "request_failed"
        assert fake.uploads == {}
        assert any(c.method == "DELETE" and "uploadId" in str(c.url) for c in fake.r2_calls())

    def test_a_conditional_write_stays_one_put(self, fake: Fake, bucket: Bucket) -> None:
        bucket.put("cond.bin", b"x" * 17_000_000, allow_overwrite=False)
        assert not any("uploads" in str(c.url) for c in fake.r2_calls())
        assert code_of(bucket.put, "c2", b"x", allow_overwrite=False, multipart=True) == "invalid_input"

    def test_empty_body(self, fake: Fake, bucket: Bucket) -> None:
        assert bucket.put("empty", b"").size == 0
        assert fake.objects["empty"].data == b""

    def test_metadata_is_refused_before_the_request(self, fake: Fake, bucket: Bucket) -> None:
        assert code_of(bucket.put, "m", "x", metadata={"note": "café"}) == "invalid_input"
        assert "m" not in fake.objects

    def test_a_private_bucket_has_no_urls_and_private_caching(self, fake: Fake, bucket: Bucket) -> None:
        fake.visibility = "private"
        blob = bucket.put("secret", "x", cache="immutable")
        assert (blob.url, blob.versioned_url) == (None, None)
        assert fake.r2_calls()[-1].headers["cache-control"] == "private, max-age=31536000, immutable"
        assert bucket.public_url("secret") is None


class TestStream:
    def test_stream_reads_lazily(self, fake: Fake, bucket: Bucket) -> None:
        bucket.put("s.bin", b"abc" * 1000, content_type="application/x-thing")
        with bucket.stream("s.bin") as blob:
            assert blob.content_type == "application/x-thing"
            assert b"".join(blob.iter_bytes()) == b"abc" * 1000
        with pytest.raises(BlobError) as e, bucket.stream("missing"):
            pass
        assert e.value.code == "not_found"


class TestList:
    def test_pages_with_a_cursor(self, fake: Fake, bucket: Bucket) -> None:
        for i in range(3):
            bucket.put(f"l/{i}&.txt", "x")
        page = bucket.list(prefix="l/", limit=2)
        assert [b.path for b in page.blobs] == ["l/0&.txt", "l/1&.txt"]
        assert page.cursor is not None
        assert page.blobs[0].uploaded_at.year == 2026
        rest = bucket.list(prefix="l/", limit=2, cursor=page.cursor)
        assert [b.path for b in rest.blobs] == ["l/2&.txt"] and rest.cursor is None


class TestSigned:
    def test_read_url_is_signed_by_the_agent(self, fake: Fake, bucket: Bucket) -> None:
        read = bucket.signed_read_url("dir/secret.txt", expires_in="2m", download_as="Report Q3.txt")
        assert fake.presigns[0]["expiresIn"] == 120
        assert fake.presigns[0]["query"]["response-content-disposition"].startswith(
            'attachment; filename="Report Q3.txt"'
        )
        assert "X-Amz-Expires=120" in read.url
        bucket.signed_read_url("a", expires_in="1h")
        bucket.signed_read_url("a")
        assert [p["expiresIn"] for p in fake.presigns[1:]] == [600, 300]
        # Nothing is signed here, so nothing needs the temporary credential.
        assert fake.mints == 0
        assert fake.calls[0].headers["authorization"] == f"Bearer {TOKEN}"

    def test_upload_url_pins_its_headers(self, fake: Fake, bucket: Bucket) -> None:
        up = bucket.signed_upload_url(
            "u/r.pdf", content_type="application/pdf", size="1kb", metadata={"owner": "u7"}, allow_overwrite=False
        )
        assert up.headers == {
            "content-type": "application/pdf",
            "cache-control": "public, max-age=3600",
            "x-amz-meta-owner": "u7",
            "content-length": "1000",
            "if-none-match": "*",
        }
        assert fake.presigns[0] == {"method": "PUT", "key": "u/r.pdf", "expiresIn": 600, "headers": up.headers}

    def test_refusals(self, fake: Fake, bucket: Bucket) -> None:
        assert code_of(bucket.signed_read_url, "dir/") == "invalid_input"
        assert fake.presigns == []
        fake.presign_response = lambda b: httpx.Response(403, json={"error": "bucket suspended"})
        with pytest.raises(BlobError) as e:
            bucket.signed_read_url("a")
        assert (e.value.code, e.value.reason) == ("forbidden", "bucket suspended")
        assert "suspended" not in e.value.message
        fake.presign_response = lambda b: httpx.Response(200, json={"url": "https://evil.example/x", "expiresAt": 1})
        assert code_of(bucket.signed_read_url, "a") == "request_failed"
        calls = len(fake.presigns)
        fake.presign_response = lambda b: httpx.Response(502)
        assert code_of(bucket.signed_read_url, "a") == "request_failed"
        assert len(fake.presigns) - calls == 3
        assert code_of(bucket.signed_read_url, "a", content_type="text/html\r\nx: y") == "invalid_input"


class TestDelete:
    def test_one_many_prefix(self, fake: Fake, bucket: Bucket) -> None:
        for k in ("d/1", "d/2", "d/3", "e/1"):
            bucket.put(k, "x")
        bucket.delete("e/1")
        bucket.delete("e/1")
        bucket.delete(["d/1", "d/2"])
        assert sorted(fake.objects) == ["d/3"]
        bucket.delete(prefix="d/")
        assert fake.objects == {}
        with pytest.raises(ValueError):
            bucket.delete(["d/1", "../x"])

    def test_the_whole_bucket_has_to_be_meant(self, fake: Fake, bucket: Bucket) -> None:
        bucket.put("keep", "x")
        assert code_of(bucket.delete, prefix="") == "invalid_input"
        assert code_of(bucket.delete) == "invalid_input"
        assert "keep" in fake.objects
        bucket.delete(prefix="", all=True)
        assert fake.objects == {}

    def test_partial_delete_names_the_survivors(self, fake: Fake, bucket: Bucket) -> None:
        bucket.put("stuck", "x")
        bucket.put("gone", "x")

        def refuse(r: httpx.Request) -> Optional[httpx.Response]:
            if r.method == "POST" and "delete" in str(r.url):
                fake.objects.pop("gone")
                body = "<DeleteResult><Error><Key>stuck</Key><Code>AccessDenied</Code></Error></DeleteResult>"
                return httpx.Response(200, content=body.encode())
            return None

        fake.intercept = refuse
        with pytest.raises(BlobError) as e:
            bucket.delete(["stuck", "gone"])
        assert (e.value.code, e.value.failed) == ("partial_delete", ["stuck"])


class TestCopyMove:
    def test_plain_copy_does_not_read_the_source(self, fake: Fake, bucket: Bucket) -> None:
        bucket.put("src.txt", "copyme", content_type="text/plain", metadata={"origin": "src"})
        fake.calls.clear()
        blob = bucket.copy("src.txt", "dst.txt")
        assert blob.size == 6
        assert [c.method for c in fake.r2_calls()] == ["PUT", "HEAD"]
        assert fake.r2_calls()[0].headers["x-amz-copy-source"] == "/bkt/src.txt"

    def test_one_option_carries_the_other_two(self, fake: Fake, bucket: Bucket) -> None:
        bucket.put("src.txt", "copyme", content_type="text/plain", cache="1m", metadata={"origin": "src"})
        bucket.copy("src.txt", "typed.md", content_type="text/markdown")
        typed = bucket.info("typed.md")
        assert (typed.content_type, typed.metadata) == ("text/markdown", {"origin": "src"})
        assert fake.objects["typed.md"].headers["cache-control"] == "public, max-age=60"
        bucket.move("typed.md", "owned.md", metadata={"owner": "u7"})
        owned = bucket.info("owned.md")
        assert (owned.content_type, owned.metadata) == ("text/markdown", {"owner": "u7"})
        assert "typed.md" not in fake.objects
        assert code_of(bucket.copy, "missing", "x") == "not_found"
        assert code_of(bucket.copy, "missing", "x", cache="1m") == "not_found"

    def test_a_failed_delete_leaves_a_copy(self, fake: Fake, bucket: Bucket) -> None:
        bucket.put("a", "x")
        fake.intercept = lambda r: error(400, "Nope") if r.method == "DELETE" else None
        assert code_of(bucket.move, "a", "b") == "move_left_a_copy"
        assert "a" in fake.objects and "b" in fake.objects


class TestUpdateJson:
    def test_create_then_compare_and_set(self, fake: Fake, bucket: Bucket) -> None:
        bucket.update_json("s.json", lambda prev: {**(prev or {}), "a": 1})
        bucket.update_json("s.json", lambda prev: {**prev, "b": 2})
        assert json.loads(fake.objects["s.json"].data) == {"a": 1, "b": 2}
        assert fake.objects["s.json"].headers["content-type"] == "application/json"

        calls = 0

        def racing(prev: Any) -> Any:
            nonlocal calls
            calls += 1
            if calls == 1:
                bucket.put("s.json", json.dumps({**prev, "raced": True}), metadata={"owner": "u9"})
            return {**prev, "c": 3}

        bucket.update_json("s.json", racing)
        assert calls == 2
        assert json.loads(fake.objects["s.json"].data) == {"a": 1, "b": 2, "raced": True, "c": 3}
        assert bucket.info("s.json").metadata == {"owner": "u9"}

    def test_max_attempts(self, fake: Fake, bucket: Bucket) -> None:
        n = 0

        def always(prev: Any) -> Any:
            nonlocal n
            n += 1
            bucket.put("m.json", json.dumps({"n": n}))
            return {"n": 0}

        with pytest.raises(BlobError) as e:
            bucket.update_json("m.json", always, max_attempts=3)
        assert (e.value.code, n) == ("conflict", 3)
        assert "3 attempts" in e.value.message
        assert code_of(bucket.update_json, "m.json", lambda p: p, max_attempts=0) == "invalid_input"


class TestMultipartUploads:
    def test_list_and_abort_stale(self, fake: Fake, bucket: Bucket) -> None:
        fake.uploads["old"] = ("tmp/a&b", {}, {}, 0.0)
        fake.uploads["new"] = ("tmp/c", {}, {}, 9e9)
        uploads = bucket.list_multipart_uploads(prefix="tmp/")
        assert {u.path for u in uploads} == {"tmp/a&b", "tmp/c"}
        stale = bucket.abort_stale_multipart_uploads(older_than="1d", prefix="tmp/")
        assert [u.upload_id for u in stale] == ["old"]
        assert list(fake.uploads) == ["new"]
        bucket.abort_multipart_upload(MultipartUpload("tmp/c", "new", uploads[0].initiated_at))
        assert fake.uploads == {}
        assert code_of(bucket.abort_multipart_upload, "tmp/c") == "invalid_input"


class TestS3:
    def test_config(self, fake: Fake, bucket: Bucket) -> None:
        cfg = bucket.s3()
        assert cfg.endpoint_url == ENDPOINT and cfg.bucket == "bkt"
        assert cfg.client_kwargs()["aws_session_token"] == "st"
        assert "sk" not in repr(cfg)


class TestAsync:
    async def test_round_trip(self, fake: Fake, abucket: AsyncBucket) -> None:
        async with abucket:
            blob = await abucket.put("a.txt", "hello", content_type="text/plain")
            assert (await abucket.get("a.txt")).text() == "hello"
            assert (await abucket.info("a.txt")).etag == blob.etag
            assert await abucket.exists("a.txt")
            async with abucket.stream("a.txt") as s:
                assert b"".join([c async for c in s.aiter_bytes()]) == b"hello"
            assert [b.path for b in (await abucket.list()).blobs] == ["a.txt"]
            await abucket.delete(prefix="", all=True)
            assert fake.objects == {}

    async def test_async_iterable_multipart(self, fake: Fake, abucket: AsyncBucket) -> None:
        async def gen() -> Any:
            for _ in range(11):
                yield b"q" * 1_000_000

        blob = await abucket.put("big", gen(), size=11_000_000, multipart=True)
        assert blob.etag.endswith('-3"') and len(fake.objects["big"].data) == 11_000_000
        assert fake.uploads == {}

    async def test_async_update_json_awaits_the_callback(self, fake: Fake, abucket: AsyncBucket) -> None:
        async def bump(prev: Any) -> Any:
            return {"n": (prev or {"n": 0})["n"] + 1}

        await abucket.update_json("n.json", bump)
        await abucket.update_json("n.json", bump)
        assert json.loads(fake.objects["n.json"].data) == {"n": 2}

    async def test_async_retries_and_refusals(self, fake: Fake, abucket: AsyncBucket) -> None:
        fake.intercept = lambda r: error(500, "InternalError")
        with pytest.raises(BlobError) as e:
            await abucket.exists("a")
        assert e.value.code == "request_failed" and len(fake.r2_calls()) == 3
        fake.intercept = None
        with pytest.raises(BlobError) as e:
            await abucket.put("x", b"x" * 3000, max_size="2kb")
        assert e.value.code == "too_large"
        read = await abucket.signed_read_url("a")
        assert read.url.startswith(ENDPOINT)
        cfg = await abucket.s3()
        assert cfg.bucket == "bkt"


class TestReviewRegressions:
    def test_a_conditional_put_is_sent_once(self, fake: Fake, bucket: Bucket) -> None:
        # The write lands, the answer is lost: a retry would 412 against the SDK's own write.
        fake.objects.clear()
        dropped = []

        def lose_reply(r: httpx.Request) -> Optional[httpx.Response]:
            if r.method == "PUT" and not dropped:
                dropped.append(fake.r2(r))
                raise httpx.ReadTimeout("lost", request=r)
            return None

        fake.intercept = lose_reply
        assert code_of(bucket.put, "c.json", "{}", allow_overwrite=False) == "request_failed"
        assert len([c for c in fake.r2_calls() if c.method == "PUT"]) == 1

    def test_update_json_does_not_run_fn_twice_on_a_lost_reply(self, fake: Fake, bucket: Bucket) -> None:
        bucket.put("n.json", '{"n":0}')
        lost = []

        def lose_reply(r: httpx.Request) -> Optional[httpx.Response]:
            if r.method == "PUT" and "if-match" in r.headers and not lost:
                lost.append(fake.r2(r))
                raise httpx.ReadTimeout("lost", request=r)
            return None

        fake.intercept = lose_reply
        with pytest.raises(BlobError):
            bucket.update_json("n.json", lambda prev: {"n": prev["n"] + 1})
        assert json.loads(fake.objects["n.json"].data) == {"n": 1}

    def test_a_token_with_a_trailing_newline(self, fake: Fake) -> None:
        make(fake, TOKEN + "\n").exists("a")
        assert fake.calls[0].headers["authorization"] == f"Bearer {TOKEN}"

    def test_a_dropped_mint_is_retried_and_the_token_stays_out_of_the_cause(self, fake: Fake, bucket: Bucket) -> None:
        answers: list = [httpx.ConnectError("x"), httpx.Response(200, json=fake.creds())]

        def mint() -> httpx.Response:
            a = answers.pop(0)
            if isinstance(a, Exception):
                raise a
            return a

        fake.mint_response = mint
        assert bucket.exists("a") is False
        reset_credential_caches()
        fake.mint_response = lambda: (_ for _ in ()).throw(httpx.ConnectTimeout("slow"))
        with pytest.raises(BlobError) as e:
            make(fake).exists("a")
        assert e.value.__cause__ is None and e.value.__suppress_context__

    def test_a_streamed_upload_that_hits_an_expired_credential_clears_it(self, fake: Fake, bucket: Bucket) -> None:
        bucket.exists("warm")
        fake.intercept = lambda r: error(403, "ExpiredToken") if r.method == "PUT" else None
        assert code_of(bucket.put, "s", iter([b"abc"]), size=3) == "unauthorized"
        fake.intercept = None
        assert bucket.put("s", iter([b"abc"]), size=3).size == 3
        assert fake.mints == 2

    def test_headers_httpx_would_refuse_are_invalid_input_not_a_network_error(self, fake: Fake, bucket: Bucket) -> None:
        assert code_of(bucket.put, "m", "x", metadata={"a": " x"}) == "invalid_input"
        assert code_of(bucket.put, "m", "x", content_type="text/plain\r\nx: y") == "invalid_input"
        assert code_of(bucket.put, "m", "x", content_type="tëxt/plain") == "invalid_input"
        assert code_of(bucket.put, "m", "x", if_unchanged='"a"\r\nx: y') == "invalid_input"
        assert fake.r2_calls() == []
        bucket.put("src", "x")
        assert code_of(bucket.copy, "src", "dst", content_type="a\nb") == "invalid_input"

    def test_a_stream_that_ends_short_or_runs_long(self, fake: Fake, bucket: Bucket) -> None:
        assert code_of(bucket.put, "short", iter([b"ab"]), size=3) == "invalid_input"
        assert code_of(bucket.put, "long", iter([b"abcd"]), size=3) == "invalid_input"
        assert "short" not in fake.objects and "long" not in fake.objects

    def test_a_listed_key_with_a_dot_segment_does_not_break_the_page(self, fake: Fake, bucket: Bucket) -> None:
        fake.objects["a/../b"] = fake.objects.get("x") or __import__("tests.fake", fromlist=["Obj"]).Obj(
            b"", {}, '"e"', 0
        )
        assert [b.path for b in bucket.list().blobs] == ["a/../b"]

    def test_an_unparseable_initiated_date_is_never_stale(self, fake: Fake, bucket: Bucket) -> None:
        body = (
            "<ListMultipartUploadsResult><IsTruncated>false</IsTruncated>"
            "<Upload><Key>k</Key><UploadId>u</UploadId><Initiated>garbage</Initiated></Upload>"
            "</ListMultipartUploadsResult>"
        )
        fake.intercept = lambda r: httpx.Response(200, content=body.encode()) if "uploads" in str(r.url) else None
        assert bucket.abort_stale_multipart_uploads(older_than=0) == []
        assert bucket.list_multipart_uploads()[0].initiated_at is None


class TestAsyncBodies:
    async def test_an_async_file_is_read_in_chunks_not_lines(self, fake: Fake, abucket: AsyncBucket) -> None:
        class AsyncFile:
            name = "photo.png"

            def __init__(self, data: bytes) -> None:
                self._f = io.BytesIO(data)

            async def read(self, n: int = -1) -> bytes:
                return self._f.read(n)

            def __aiter__(self) -> Any:
                raise AssertionError("read line by line")

        data = PNG + b"\x00" * 200_000
        blob = await abucket.put("a.png", AsyncFile(data), size=len(data))
        assert blob.content_type == "image/png"
        assert fake.objects["a.png"].data == data

    async def test_a_path_body(self, fake: Fake, abucket: AsyncBucket, tmp_path: Path) -> None:
        f = tmp_path / "doc.pdf"
        f.write_bytes(b"%PDF-1.7" + b"x" * 100_000)
        blob = await abucket.put("doc.pdf", f, content_types=["application/pdf"])
        assert (blob.content_type, blob.size) == ("application/pdf", 100_008)
