import json
import os
import re
import time
from typing import Any, Iterator
from urllib.parse import parse_qs, quote, unquote, urlsplit

import boto3
import httpx
import pytest

from tests.live.conftest import ROOT, p
from upstash_blob import AsyncBucket, BlobError, Bucket

pytestmark = pytest.mark.live

PNG = bytes([0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0, 0, 0, 0x0D, 0x49, 0x48, 0x44, 0x52, 0, 0, 0, 1])


def data(n: int, seed: int = 1) -> bytes:
    return bytes((i * 31 + seed) & 0xFF for i in range(n))


def code_of(fn: Any, *args: Any, **kwargs: Any) -> str:
    with pytest.raises(BlobError) as e:
        fn(*args, **kwargs)
    return e.value.code


class TestPut:
    def test_bytes_then_get_info_exists(self, pub: Bucket) -> None:
        blob = pub.put(p("a.txt"), "hello", content_type="text/plain", metadata={"uploadedby": "u1"})
        assert blob.size == 5
        assert re.fullmatch(r'"[0-9a-f]{32}"', blob.etag)
        assert re.match(r"https://b[0-9a-f]{11}\.blob\.upstash\.io/test/", blob.url or "")
        assert blob.versioned_url == f"{blob.url}?v={quote(blob.etag, safe='')}"
        got = pub.get(p("a.txt"))
        assert (got.text(), got.content_type, got.metadata["uploadedby"], got.etag) == (
            "hello",
            "text/plain",
            "u1",
            blob.etag,
        )
        assert pub.info(p("a.txt")).size == 5
        assert pub.exists(p("a.txt")) and not pub.exists(p("nope"))

    def test_public_url_serves_with_the_chosen_cache(self, pub: Bucket) -> None:
        blob = pub.put(p("served.txt"), "served", content_type="text/plain", cache="1m")
        res = httpx.get(blob.url or "")
        assert (res.status_code, res.text) == (200, "served")
        assert "max-age=60" in res.headers["cache-control"]
        assert pub.public_url(p("served.txt")) == blob.url

    def test_sniffing_and_content_types(self, pub: Bucket) -> None:
        assert pub.put(p("x.png"), PNG, content_type="image/png", content_types=["image/*"]).size == len(PNG)
        lie = b"PK\x03\x04\x14\x00"
        assert code_of(pub.put, p("lie.png"), lie, content_type="image/png", content_types=["image/png"]) == (
            "content_type_not_allowed"
        )
        assert code_of(pub.put, p("x.svg"), "<svg/>", content_type="image/svg+xml", content_types=["image/*"]) == (
            "content_type_not_allowed"
        )
        assert not pub.exists(p("lie.png"))

    def test_streams_files_and_iterators(self, pub: Bucket, tmp_path: Any) -> None:
        body = data(70_000, 7)
        f = tmp_path / "blob.bin"
        f.write_bytes(body)
        with open(f, "rb") as handle:
            assert pub.put(p("file.bin"), handle, max_size="1mb").size == 70_000
        assert pub.get(p("file.bin")).content == body

        def chunks() -> Iterator[bytes]:
            for i in range(0, len(body), 8192):
                yield body[i : i + 8192]

        assert pub.put(p("sized.bin"), chunks(), size=len(body)).size == len(body)
        assert pub.put(p("buffered.bin"), chunks(), max_size="100kb").size == len(body)
        assert pub.get(p("sized.bin")).content == body
        assert code_of(pub.put, p("nolen"), chunks()) == "length_required"
        assert code_of(pub.put, p("big"), data(3000), max_size="2kb") == "too_large"

    def test_conditional_writes(self, pub: Bucket) -> None:
        first = pub.put(p("once"), "one")
        with pytest.raises(BlobError) as e:
            pub.put(p("once"), "two", allow_overwrite=False)
        assert (e.value.code, e.value.status, e.value.etag, e.value.size) == ("already_exists", 409, first.etag, 3)
        assert code_of(pub.put, p("once"), "three", if_unchanged='"deadbeef"') == "conflict"
        assert pub.put(p("once"), "four", if_unchanged=first.etag).size == 4

    def test_metadata_guard(self, pub: Bucket) -> None:
        assert code_of(pub.put, p("meta-bad.txt"), "x", metadata={"note": "café"}) == "invalid_input"
        assert not pub.exists(p("meta-bad.txt"))
        pub.put(p("meta-ok.txt"), "x", metadata={"note": quote("café")})
        assert unquote(pub.info(p("meta-ok.txt")).metadata["note"]) == "café"


class TestRead:
    def test_not_found(self, pub: Bucket) -> None:
        assert code_of(pub.get, p("missing")) == "not_found"
        assert code_of(pub.info, p("missing")) == "not_found"

    def test_list_pages(self, priv: Bucket) -> None:
        for i in range(3):
            priv.put(p(f"list/{i}.txt"), f"n{i}")
        page1 = priv.list(prefix=p("list/"), limit=2)
        assert len(page1.blobs) == 2 and page1.cursor
        assert page1.blobs[0].url is None
        assert page1.blobs[0].uploaded_at.timestamp() > time.time() - 600
        page2 = priv.list(prefix=p("list/"), limit=2, cursor=page1.cursor)
        assert len(page2.blobs) == 1 and page2.cursor is None

    def test_stream(self, priv: Bucket) -> None:
        body = data(300_000, 3)
        priv.put(p("stream.bin"), body)
        with priv.stream(p("stream.bin")) as blob:
            assert blob.size == len(body)
            assert b"".join(blob.iter_bytes()) == body

    def test_signed_read_url(self, priv: Bucket) -> None:
        priv.put(p("secret.txt"), "shh", content_type="text/plain")
        assert priv.public_url(p("secret.txt")) is None
        read = priv.signed_read_url(p("secret.txt"), expires_in=120, download_as="Report Q3.txt")
        params = parse_qs(urlsplit(read.url).query)
        assert params["X-Amz-Expires"] == ["120"]
        assert "X-Amz-Security-Token" not in params
        assert read.expires_at.timestamp() > time.time() + 110
        res = httpx.get(read.url)
        assert (res.status_code, res.text) == (200, "shh")
        assert "Report Q3.txt" in res.headers["content-disposition"]
        assert httpx.get(read.url.replace("secret.txt", "other.txt")).status_code == 403
        capped = priv.signed_read_url(p("secret.txt"), expires_in="1h")
        assert parse_qs(urlsplit(capped.url).query)["X-Amz-Expires"] == ["600"]

    def test_signed_upload_url(self, priv: Bucket) -> None:
        up = priv.signed_upload_url(p("up.txt"), content_type="text/plain", metadata={"owner": "u7"}, size=5)
        assert httpx.put(up.url, content=b"hello").status_code == 403
        assert (
            httpx.put(up.url, headers={**up.headers, "content-type": "text/html"}, content=b"hello").status_code == 403
        )
        assert httpx.put(up.url, headers=up.headers, content=b"hello").status_code == 200
        info = priv.info(p("up.txt"))
        assert (info.size, info.content_type, info.metadata) == (5, "text/plain", {"owner": "u7"})
        priv.put(p("taken.txt"), "first")
        taken = priv.signed_upload_url(p("taken.txt"), content_type="text/plain", allow_overwrite=False)
        assert httpx.put(taken.url, headers=taken.headers, content=b"second").status_code == 412
        assert priv.get(p("taken.txt")).text() == "first"


class TestLarge:
    def test_multipart_and_nothing_left_behind(self, priv: Bucket) -> None:
        size = 17_000_000
        body = os.urandom(size)
        blob = priv.put(p("multipart/big.bin"), body, content_type="application/octet-stream")
        assert blob.size == size and blob.etag.endswith('-4"')
        assert priv.list_multipart_uploads(prefix=p("multipart/")) == []
        assert priv.get(p("multipart/big.bin")).content == body
        once = priv.put(p("multipart/cond.bin"), body, allow_overwrite=False)
        assert not re.search(r'-\d+"$', once.etag)
        assert code_of(priv.put, p("multipart/cond.bin"), body, allow_overwrite=False) == "already_exists"

    def test_stale_uploads_are_listed_and_aborted(self, priv: Bucket) -> None:
        priv._create_multipart(p("stale/x.bin"), {"content-type": "application/octet-stream"})
        # R2 lists an upload under a different id than create answered with; both abort it.
        listed = priv.list_multipart_uploads(prefix=p("stale/"))
        assert [u.path for u in listed] == [p("stale/x.bin")]
        assert priv.abort_stale_multipart_uploads(older_than="1h", prefix=p("stale/")) == []
        priv.abort_multipart_upload(listed[0])
        assert priv.list_multipart_uploads(prefix=p("stale/")) == []


class TestWrite:
    def test_copy_move(self, priv: Bucket) -> None:
        priv.put(p("c/src.txt"), "copyme", content_type="text/plain", metadata={"origin": "src"})
        assert priv.copy(p("c/src.txt"), p("c/plain.txt")).size == 6
        assert priv.info(p("c/plain.txt")).metadata == {"origin": "src"}
        priv.copy(p("c/src.txt"), p("c/typed.md"), content_type="text/markdown")
        typed = priv.info(p("c/typed.md"))
        assert (typed.content_type, typed.metadata) == ("text/markdown", {"origin": "src"})
        priv.move(p("c/typed.md"), p("c/owned.md"), metadata={"owner": "u7"})
        owned = priv.info(p("c/owned.md"))
        assert (owned.content_type, owned.metadata) == ("text/markdown", {"owner": "u7"})
        assert not priv.exists(p("c/typed.md"))
        assert code_of(priv.copy, p("missing"), p("x")) == "not_found"

    def test_delete(self, pub: Bucket) -> None:
        for k in ("d/1", "d/2", "d/3", "e/1"):
            pub.put(p(k), "x")
        pub.delete(p("e/1"))
        pub.delete(p("e/1"))
        pub.delete([p("d/1"), p("d/2")])
        assert [b.path for b in pub.list(prefix=p("d/")).blobs] == [p("d/3")]
        pub.delete(prefix=p("d/"))
        assert pub.list(prefix=p("d/")).blobs == []
        assert code_of(pub.delete, prefix="") == "invalid_input"
        assert pub.list(prefix=ROOT).blobs

    def test_update_json(self, priv: Bucket) -> None:
        priv.update_json(p("s.json"), lambda prev: {**(prev or {}), "a": 1})
        priv.update_json(p("s.json"), lambda prev: {**prev, "b": 2})
        calls = 0

        def racing(prev: Any) -> Any:
            nonlocal calls
            calls += 1
            if calls == 1:
                priv.put(p("s.json"), json.dumps({**prev, "raced": True}), content_type="application/json")
            return {**prev, "c": 3}

        priv.update_json(p("s.json"), racing)
        assert calls == 2
        got = priv.get(p("s.json"))
        assert got.content_type == "application/json"
        assert got.json() == {"a": 1, "b": 2, "raced": True, "c": 3}


class TestS3:
    def test_boto3(self, priv: Bucket) -> None:
        priv.put(p("s3/one.txt"), "x")
        cfg = priv.s3()
        s3 = boto3.client("s3", **cfg.client_kwargs())
        out = s3.list_objects_v2(Bucket=cfg.bucket, Prefix=p("s3/"), MaxKeys=10)
        assert [c["Key"] for c in out["Contents"]] == [p("s3/one.txt")]
        assert cfg.expires_at.timestamp() > time.time()


class TestAsync:
    async def test_round_trip(self, priv: Bucket) -> None:
        async with AsyncBucket.from_env("UPSTASH_BLOB_PRIVATE_TOKEN") as a:
            body = os.urandom(11_000_000)

            async def chunks() -> Any:
                for i in range(0, len(body), 1_000_000):
                    yield body[i : i + 1_000_000]

            blob = await a.put(p("async/big.bin"), chunks(), size=len(body), multipart=True)
            assert blob.etag.endswith('-3"')
            async with a.stream(p("async/big.bin")) as s:
                assert b"".join([c async for c in s.aiter_bytes()]) == body
            assert (await a.signed_read_url(p("async/big.bin"))).url.startswith("https://")
            await a.delete(prefix=p("async/"))
            assert (await a.list(prefix=p("async/"))).blobs == []
