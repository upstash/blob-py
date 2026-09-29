import json
from typing import Any, Dict, List

import httpx
import pytest

from tests.live.conftest import p
from upstash_blob import (
    AsyncBucket,
    AsyncUploadHandler,
    BeforeUploadResult,
    BlobError,
    Bucket,
    UploadCompleteArgs,
    UploadHandler,
)

pytestmark = pytest.mark.live

MIB = 1024 * 1024
JSON = {"content-type": "application/json"}


def data(n: int, seed: int = 7) -> bytes:
    return bytes((i * 31 + seed) & 0xFF for i in range(n))


def post(h: UploadHandler, body: Dict[str, Any]) -> Any:
    res = h.handle("POST", body=json.dumps(body).encode(), headers=JSON)
    assert res.status == 200, res.body
    return json.loads(res.body)


def browser_put(begun: Dict[str, Any], body: bytes) -> List[Dict[str, Any]]:
    """What @upstash/blob/browser does with the plan: PUT each part to its url, verbatim headers."""
    size = begun["upload"]["partSize"]
    landed = []
    for part in begun["upload"]["parts"]:
        n = part["n"]
        chunk = body[(n - 1) * size : n * size]
        res = httpx.put(part["url"], content=chunk, headers=part.get("headers", {}), timeout=60)
        assert res.status_code == 200, res.text
        landed.append({"n": n, "etag": res.headers["etag"]})
    return landed


def test_single_put_through_the_handler(pub: Bucket) -> None:
    seen: List[UploadCompleteArgs] = []
    h = UploadHandler(
        bucket=pub,
        content_types=["text/plain"],
        on_before_upload=lambda a: BeforeUploadResult(path=p(f"h/{a.file.name}"), metadata={"by": "u1"}),
        on_upload_complete=lambda a: seen.append(a) or {"ok": True},
    )
    body = b"hello from the handler"
    begun = post(h, {"phase": "begin", "file": {"name": "a.txt", "type": "text/plain", "size": len(body)}})
    assert begun["upload"]["multipart"] is False
    landed = browser_put(begun, body)
    done = post(h, {"phase": "end", "completionToken": begun["completionToken"], "parts": landed})
    assert done["data"] == {"ok": True}
    assert done["blob"]["size"] == len(body) and done["blob"]["contentType"] == "text/plain"
    assert httpx.get(done["blob"]["url"]).content == body
    assert seen[0].metadata == {"by": "u1"}
    got = pub.get(p("h/a.txt"))
    assert got.content == body and got.metadata["by"] == "u1"


def test_a_tampered_put_is_refused_by_storage(pub: Bucket) -> None:
    h = UploadHandler(bucket=pub, on_before_upload=lambda a: BeforeUploadResult(path=p("h/tamper.txt")))
    begun = post(h, {"phase": "begin", "file": {"name": "t.txt", "type": "text/plain", "size": 5}})
    part = begun["upload"]["parts"][0]
    headers = {**part["headers"], "x-amz-meta-upstash-upload": "someone-else"}
    assert httpx.put(part["url"], content=b"hello", headers=headers).status_code == 403
    assert httpx.put(part["url"], content=b"hello!", headers=part["headers"]).status_code == 403


def test_refusal_at_end_deletes_the_object(priv: Bucket) -> None:
    def refuse(a: UploadCompleteArgs) -> None:
        raise BlobError("forbidden", "no")

    h = UploadHandler(
        bucket=priv, on_before_upload=lambda a: BeforeUploadResult(path=p("h/refused.txt")), on_upload_complete=refuse
    )
    begun = post(h, {"phase": "begin", "file": {"name": "r.txt", "type": "text/plain", "size": 3}})
    assert begun["upload"]["parts"][0]["headers"]["cache-control"].startswith("private")
    landed = browser_put(begun, b"abc")
    assert priv.exists(p("h/refused.txt"))
    body = {"phase": "end", "completionToken": begun["completionToken"], "parts": landed}
    res = h.handle("POST", body=json.dumps(body).encode(), headers=JSON)
    assert res.status == 403
    assert not priv.exists(p("h/refused.txt"))


def test_multipart_through_the_handler_with_resume(pub: Bucket) -> None:
    size = 6 * MIB + 123
    body = data(size)
    h = UploadHandler(
        bucket=pub,
        multipart=True,
        on_before_upload=lambda a: BeforeUploadResult(path=p("h/big.bin"), metadata={"k": "v"}),
    )
    begun = post(h, {"phase": "begin", "file": {"name": "big.bin", "type": "application/octet-stream", "size": size}})
    plan = begun["upload"]
    assert plan["multipart"] is True and plan["partSize"] == 5 * MIB and len(plan["parts"]) == 2
    first = browser_put({**begun, "upload": {**plan, "parts": plan["parts"][:1]}}, body)
    resumed = post(h, {"phase": "parts", "completionToken": begun["completionToken"], "from": 2})
    assert resumed["landed"] == first
    rest = browser_put({**begun, "upload": {**plan, "parts": resumed["parts"]}}, body)
    done = post(h, {"phase": "end", "completionToken": begun["completionToken"], "parts": first + rest})
    assert done["blob"]["size"] == size
    got = pub.get(p("h/big.bin"))
    assert got.content == body and got.metadata["k"] == "v"


def test_multipart_cancel_aborts(pub: Bucket) -> None:
    h = UploadHandler(bucket=pub, multipart=True, on_before_upload=lambda a: BeforeUploadResult(path=p("h/cancel.bin")))
    begun = post(h, {"phase": "begin", "file": {"name": "c", "type": "", "size": 6 * MIB}})
    assert any(u.path == p("h/cancel.bin") for u in pub.list_multipart_uploads(prefix=p("h/")))
    post(h, {"phase": "cancel", "completionToken": begun["completionToken"]})
    assert not any(u.path == p("h/cancel.bin") for u in pub.list_multipart_uploads(prefix=p("h/")))


async def test_async_handler_single_put() -> None:
    import os

    if not os.environ.get("UPSTASH_BLOB_PUBLIC_TOKEN"):
        pytest.skip("UPSTASH_BLOB_PUBLIC_TOKEN is not set")
    async with AsyncBucket.from_env("UPSTASH_BLOB_PUBLIC_TOKEN") as bucket:
        h = AsyncUploadHandler(bucket=bucket, on_before_upload=lambda a: BeforeUploadResult(path=p("h/async.txt")))
        body = b"async hello"
        file = {"name": "x.txt", "type": "text/plain", "size": len(body)}
        res = await h.handle("POST", body=json.dumps({"phase": "begin", "file": file}).encode(), headers=JSON)
        begun = json.loads(res.body)
        landed = browser_put(begun, body)
        end = {"phase": "end", "completionToken": begun["completionToken"], "parts": landed}
        res = await h.handle("POST", body=json.dumps(end).encode(), headers=JSON)
        assert res.status == 200, res.body
        assert (await bucket.get(p("h/async.txt"))).content == body
