"""A scripted agent and an in-memory R2, behind an httpx.MockTransport shared by both buckets."""

import hashlib
import json
import time
from dataclasses import dataclass, field
from email.utils import formatdate
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, quote, unquote, urlsplit

import httpx

from upstash_blob._token import encode_token

TOKEN = encode_token("bucket-id", "pw-secret", "b0123456789a")
OTHER_TOKEN = encode_token("bucket-id", "pw-secret", "bffffffffffe")
ENDPOINT = "https://acc.r2.cloudflarestorage.com"


@dataclass
class Obj:
    data: bytes
    headers: Dict[str, str]
    etag: str
    modified: float


@dataclass
class Fake:
    objects: Dict[str, Obj] = field(default_factory=dict)
    uploads: Dict[str, Tuple[str, Dict[str, str], Dict[int, bytes], float]] = field(default_factory=dict)
    calls: List[httpx.Request] = field(default_factory=list)
    mints: int = 0
    presigns: List[Dict[str, Any]] = field(default_factory=list)
    visibility: Optional[str] = None
    ttl: int = 600
    # Return a response to answer a request before the fake bucket sees it.
    intercept: Optional[Callable[[httpx.Request], Optional[httpx.Response]]] = None
    mint_response: Optional[Callable[[], httpx.Response]] = None
    presign_response: Optional[Callable[[Dict[str, Any]], httpx.Response]] = None
    next_upload: int = 0

    def r2_calls(self) -> List[httpx.Request]:
        return [c for c in self.calls if str(c.url).startswith(ENDPOINT)]

    def creds(self) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "accessKeyId": "AKIAOBJECT",
            "secretAccessKey": "sk",
            "sessionToken": "st",
            "endpoint": ENDPOINT,
            "bucket": "bkt",
            "region": "auto",
            "expiresAt": int(time.time()) + self.ttl,
        }
        if self.visibility:
            body["visibility"] = self.visibility
        return body

    def handler(self, request: httpx.Request) -> httpx.Response:
        # A streamed body has to be drained here or the client never finishes sending it.
        request.read()
        self.calls.append(request)
        url = str(request.url)
        if url == "https://blob.upstash.io/v1/credentials":
            self.mints += 1
            return self.mint_response() if self.mint_response else httpx.Response(200, json=self.creds())
        if url == "https://blob.upstash.io/v1/presign":
            body = json.loads(request.content)
            self.presigns.append(body)
            return self.presign_response(body) if self.presign_response else self.agent_presign(body)
        if self.intercept:
            res = self.intercept(request)
            if res is not None:
                return res
        return self.r2(request)

    async def ahandler(self, request: httpx.Request) -> httpx.Response:
        await request.aread()
        return self.handler(request)

    def agent_presign(self, body: Dict[str, Any]) -> httpx.Response:
        url = f"{ENDPOINT}/bucket-id/{quote(body['key'])}?X-Amz-Expires={body['expiresIn']}"
        return httpx.Response(200, json={"url": url, "expiresAt": int(time.time()) + body["expiresIn"]})

    # ------------------------------------------------------------------ R2

    def r2(self, req: httpx.Request) -> httpx.Response:
        parts = urlsplit(str(req.url))
        assert parts.path.startswith("/bkt"), parts.path
        key = unquote(parts.path[len("/bkt/") :]) if parts.path.startswith("/bkt/") else None
        q = dict(parse_qsl(parts.query, keep_blank_values=True))
        assert req.headers["x-amz-content-sha256"] == "UNSIGNED-PAYLOAD"
        assert req.headers["authorization"].startswith("AWS4-HMAC-SHA256 Credential=AKIAOBJECT/")
        m = req.method

        if key is None:
            if m == "GET" and "uploads" in q:
                return self.list_uploads(q)
            if m == "GET":
                return self.list(q)
            if m == "POST" and "delete" in q:
                return self.delete_many(req.content.decode())
        elif m == "POST" and "uploads" in q:
            self.next_upload += 1
            uid = f"up&{self.next_upload}"
            self.uploads[uid] = (key, self.object_headers(req), {}, time.time())
            return xml(
                f"<InitiateMultipartUploadResult><UploadId>{esc(uid)}</UploadId></InitiateMultipartUploadResult>"
            )
        elif m == "PUT" and "partNumber" in q:
            up = self.uploads.get(q["uploadId"])
            if not up:
                return error(404, "NoSuchUpload")
            up[2][int(q["partNumber"])] = req.content
            return httpx.Response(200, headers={"etag": etag_of(req.content)})
        elif m == "POST" and "uploadId" in q:
            up = self.uploads.pop(q["uploadId"], None)
            if not up:
                return error(404, "NoSuchUpload")
            data = b"".join(up[2][n] for n in sorted(up[2]))
            self.objects[key] = Obj(data, up[1], f'"{hashlib.md5(data).hexdigest()}-{len(up[2])}"', time.time())
            return xml(
                f"<CompleteMultipartUploadResult><ETag>{esc(self.objects[key].etag)}</ETag></CompleteMultipartUploadResult>"
            )
        elif m == "GET" and "uploadId" in q:
            up = self.uploads.get(q["uploadId"])
            if not up:
                return error(404, "NoSuchUpload")
            items = "".join(
                f"<Part><PartNumber>{n}</PartNumber><ETag>{esc(etag_of(d))}</ETag><Size>{len(d)}</Size></Part>"
                for n, d in sorted(up[2].items())
            )
            return xml(f"<ListPartsResult><IsTruncated>false</IsTruncated>{items}</ListPartsResult>")
        elif m == "DELETE" and "uploadId" in q:
            return httpx.Response(204) if self.uploads.pop(q["uploadId"], None) else error(404, "NoSuchUpload")
        elif m == "PUT" and "x-amz-copy-source" in req.headers:
            src = unquote(req.headers["x-amz-copy-source"].split("/", 2)[2])
            obj = self.objects.get(src)
            if not obj:
                return error(404, "NoSuchKey")
            replace = req.headers.get("x-amz-metadata-directive") == "REPLACE"
            headers = self.object_headers(req) if replace else dict(obj.headers)
            self.objects[key] = Obj(obj.data, headers, obj.etag, time.time())
            return xml(f"<CopyObjectResult><ETag>{esc(obj.etag)}</ETag></CopyObjectResult>")
        elif m == "PUT":
            current = self.objects.get(key)
            if req.headers.get("if-none-match") == "*" and current:
                return error(412, "PreconditionFailed")
            if "if-match" in req.headers and (not current or current.etag != req.headers["if-match"]):
                return error(412, "PreconditionFailed")
            assert int(req.headers["content-length"]) == len(req.content)
            self.objects[key] = Obj(req.content, self.object_headers(req), etag_of(req.content), time.time())
            return httpx.Response(200, headers={"etag": self.objects[key].etag})
        elif m in ("GET", "HEAD"):
            obj = self.objects.get(key)
            if not obj:
                return error(404, "NoSuchKey") if m == "GET" else httpx.Response(404)
            headers = {
                **obj.headers,
                "etag": obj.etag,
                "content-length": str(len(obj.data)),
                "last-modified": formatdate(obj.modified, usegmt=True),
            }
            return httpx.Response(200, headers=headers, content=obj.data if m == "GET" else b"")
        elif m == "DELETE":
            self.objects.pop(key, None)
            return httpx.Response(204)
        raise AssertionError(f"unexpected {m} {req.url}")

    @staticmethod
    def object_headers(req: httpx.Request) -> Dict[str, str]:
        keep = ("content-type", "cache-control")
        return {k: v for k, v in req.headers.items() if k in keep or k.startswith("x-amz-meta-")}

    def list(self, q: Dict[str, str]) -> httpx.Response:
        keys = sorted(k for k in self.objects if k.startswith(q.get("prefix", "")))
        start = int(q.get("continuation-token", "0"))
        limit = int(q.get("max-keys", "1000"))
        page = keys[start : start + limit]
        more = start + limit < len(keys)
        contents = "".join(
            f"<Contents><Key>{esc(k)}</Key><Size>{len(self.objects[k].data)}</Size>"
            f"<ETag>{esc(self.objects[k].etag)}</ETag><LastModified>2026-09-29T10:00:00.000Z</LastModified></Contents>"
            for k in page
        )
        token = f"<NextContinuationToken>{start + limit}</NextContinuationToken>" if more else ""
        return xml(
            f"<ListBucketResult><IsTruncated>{str(more).lower()}</IsTruncated>{contents}{token}</ListBucketResult>"
        )

    def list_uploads(self, q: Dict[str, str]) -> httpx.Response:
        items = "".join(
            f"<Upload><Key>{esc(k)}</Key><UploadId>{esc(uid)}</UploadId>"
            f"<Initiated>{time.strftime('%Y-%m-%dT%H:%M:%S.000Z', time.gmtime(t))}</Initiated></Upload>"
            for uid, (k, _, _, t) in self.uploads.items()
            if k.startswith(q.get("prefix", ""))
        )
        return xml(f"<ListMultipartUploadsResult><IsTruncated>false</IsTruncated>{items}</ListMultipartUploadsResult>")

    def delete_many(self, body: str) -> httpx.Response:
        import re

        for k in re.findall("<Key>(.*?)</Key>", body):
            self.objects.pop(unesc(k), None)
        return xml("<DeleteResult></DeleteResult>")


def esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def unesc(s: str) -> str:
    return s.replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", '"').replace("&amp;", "&")


def etag_of(data: bytes) -> str:
    return f'"{hashlib.md5(data).hexdigest()}"'


def xml(body: str, status: int = 200) -> httpx.Response:
    return httpx.Response(status, content=body.encode(), headers={"content-type": "application/xml"})


def error(status: int, code: str) -> httpx.Response:
    return xml(f"<Error><Code>{code}</Code><Message>{code}</Message></Error>", status)
