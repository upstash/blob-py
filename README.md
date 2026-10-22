# upstash-blob

Python SDK for [Upstash Blob](https://upstash.com/docs/blob). Sync and async, one dependency (`httpx`).

```bash
pip install upstash-blob
```

## Usage

```python
from pathlib import Path

from upstash_blob import Bucket

bucket = Bucket.from_env()  # reads UPSTASH_BLOB_TOKEN, or Bucket(token)

blob = bucket.put("avatars/me.png", Path("me.png"))
blob.url  # None on a private bucket
blob.etag

bucket.get("avatars/me.png").content  # record + bytes
bucket.info("avatars/me.png")  # same record, no bytes
bucket.exists("avatars/me.png")  # False instead of a raise
bucket.list(prefix="avatars/", limit=100)  # ListPage(blobs, cursor)
bucket.copy("tmp/9f3c", "avatars/7.png")
bucket.move("tmp/9f3c", "avatars/7.png", content_type="image/png")
bucket.delete("a.png")  # or ["a.png", "b.png"], or prefix="tmp/"
bucket.update_json("u/7.json", lambda prev: {**(prev or {}), "theme": "dark"})

with bucket.stream("videos/intro.mp4") as video:
    for chunk in video.iter_bytes():
        ...
```

`AsyncBucket` has the same methods, awaited, and also takes async iterables as a body:

```python
from upstash_blob import AsyncBucket

async with AsyncBucket.from_env() as bucket:
    await bucket.put("report.pdf", data, content_type="application/pdf")
```

- `put` takes bytes, str, a `pathlib.Path`, a binary file object, or an iterable of bytes. The content
  type is guessed from a file name, else `application/octet-stream`. An iterable of unknown length
  needs `size=` or `max_size=`.
- `content_types=["image/*"]` checks the declared type and the file's first bytes; `max_size="20mb"`
  refuses anything larger, while streaming too.
- A body over 16 MB goes up as multipart, up to four parts at a time within a 64 MiB buffer;
  `multipart="100mb" | True | False` moves the line. A failed part aborts the upload.
  `allow_overwrite=False` and `if_unchanged=etag` are single-PUT only.
- `get` and `info` raise `not_found` rather than returning None; `list` returns keys, sizes, etags
  and urls, without metadata. `cursor` is set only while more remains.
- `update_json` is a compare-and-set loop, retried on conflict up to `max_attempts` times (default 6).
- `copy` and `move` take `content_type`, `cache`, `metadata`; whatever is not given is carried over
  from the source. `move` is a copy plus a delete: a failed delete raises `move_left_a_copy`.
- `delete(prefix="")` needs `all=True`. A partial list delete raises `partial_delete`.
- Metadata keys are lowercase (storage reads them back lowercased) and values printable ASCII;
  anything else is refused with `invalid_input`.
- `retry={"retries": 5, "backoff": lambda n: 100 * 2**n}` (milliseconds before retry `n`, from 0)
  tunes how failed requests are retried, and `retry=False` sends each once. The default is 2 retries
  with jittered backoff from 200 ms; a Retry-After from the service always wins.
- `cache` takes `"immutable"`, `"revalidate"`, `"no-store"`, a duration (`"15m"`, `3600`,
  `timedelta`), or a verbatim header.
- Sizes count bytes. Decimal units (`KB`, `MB`, `GB`, `TB`) are powers of 1,000 and binary units
  (`KiB`, `MiB`, `GiB`, `TiB`) powers of 1,024, case-insensitive: `"32MB"` is 32,000,000 bytes and
  refuses a 32 MiB file, which needs `"32MiB"` or `32 * 1024 * 1024`.

### Signed URLs

```python
read = bucket.signed_read_url("private/report.pdf", download_as="report.pdf")
upload = bucket.signed_upload_url("u/7/report.pdf", content_type="application/pdf", size=size)
httpx.put(upload.url, headers=upload.headers, content=body)
```

Links are signed by Upstash and live at most 10 minutes (read default: 5); `expires_at` is the answer
per link. `headers` on an upload URL are pinned into the signature. Whether a bucket is private is
decided in the console: a private bucket has no `url` on any blob and `public_url()` is None.

### Browser uploads

`UploadHandler` (sync) and `AsyncUploadHandler` serve the protocol of `@upstash/blob/browser` and
`@upstash/blob/react`, so a React app can upload straight to storage through a Python backend, with
multipart, resume, progress and a completion callback.

```python
from upstash_blob import AsyncUploadHandler, BeforeUploadResult, UploadRoute, unique_path
from upstash_blob.fastapi import upload_view

uploads = AsyncUploadHandler(
    context=require_user,  # its return is `ctx`; raise a 401 to refuse
    owner=lambda user: user.id,  # binds each upload to who began it
    on_before_upload=lambda a: BeforeUploadResult(path=unique_path(f"u/{a.ctx.id}/{a.file.name}")),
    on_upload_complete=lambda a: {"url": a.url},  # reaches the browser as upload.blob.data
    routes={
        "avatar": UploadRoute(content_types=["image/*"], max_size="5mb"),
        "video": UploadRoute(content_types=["video/*"], max_size="2gb"),
    },
)
app.add_api_route("/api/upload", upload_view(uploads), methods=["GET", "POST"])
```

```tsx
const upload = useUpload('avatar');  // POSTs to /api/upload?route=avatar
```

- Django: `path("api/upload", upload_view(uploads))` from `upstash_blob.django`, with either
  handler. Flask: `app.add_url_rule("/api/upload", view_func=upload_view(uploads), methods=["GET", "POST"])`
  from `upstash_blob.flask`, with `UploadHandler`. Anything else: `handler.handle(method, request=...,
  route=..., body=..., headers=..., native=<framework response class>)` returns an `UploadResponse`.
- Omit `bucket=` to use `UPSTASH_BLOB_TOKEN`. Every handler option is a default a route may replace;
  `content_types=None` or `max_size=None` on a route clears the handler's.
- `context` runs on every POST of an upload: check the session there. `owner` signs who began an
  upload into it; a later request from anyone else is a 403, so one signed-in user cannot finish,
  re-sign or cancel another's upload. An `owner` of None or `""` is a 401.
- The first upload from each page origin also sends storage the CORS preflight the browser will, and
  logs a warning on the `upstash_blob` logger when storage refuses it: add the origin to the bucket's
  CORS settings in the Upstash Console.
- `input=` validates what the browser sends as `input`: a pydantic model, a `TypeAdapter`, or a
  callable that raises `ValueError`.
- `on_upload_complete` runs at least once per upload; `upload_id` is stable across deliveries.
  Raising deletes the object. `state` rides in a signed but readable token: an id, never a secret.
- `on_error` sees every refusal; return a `BlobError` or a framework response to answer with it.
- `unique_path` adds an eight-character random suffix to the final filename and keeps everything else
  as given: `u/7/Q3 Report.pdf` becomes `u/7/Q3 Report-<random>.pdf`. A path with a `.` or `..`
  segment is refused as `invalid_input`. Store the returned path and authorize reads by it.
- The handler speaks the same versioned upload protocol as `@upstash/blob`'s `uploadHandler`, so
  any release of the browser client works against it and either side can be upgraded first.

### Incomplete uploads

A multipart upload that was never finished is invisible to `list()` and blocks bucket deletion. The
SDK aborts the ones it starts; sweep the rest from a cron:

```python
bucket.abort_stale_multipart_uploads(older_than="1d", prefix="uploads/")
```

### The S3 escape hatch

```python
cfg = bucket.s3()
s3 = boto3.client("s3", **cfg.client_kwargs())
s3.list_objects_v2(Bucket=cfg.bucket)
```

The credentials are short-lived; call `s3()` again after `cfg.expires_at`.

## Errors

Everything raises a `BlobError` with a `code` from a closed list, a `status`, and a printable
message. `e.to_dict()` is the JSON shape the TypeScript SDK's browser half decodes.

```python
try:
    bucket.put(path, body, max_size="5mb")
except BlobError as e:
    if e.code == "too_large":
        ...
```

## Telemetry

Credential and signing requests carry the SDK version, Python version, and platform. Set
`UPSTASH_DISABLE_TELEMETRY` or pass `enable_telemetry=False`.

## Development

```bash
uv sync
uv run ruff check . && uv run ruff format --check . && uv run mypy upstash_blob
uv run pytest tests/unit
```

`uv run pytest tests/live` runs against real buckets and needs `UPSTASH_BLOB_PUBLIC_TOKEN` and
`UPSTASH_BLOB_PRIVATE_TOKEN` (a `.env` file works).

## License

[MIT](./LICENSE)
