import os
import random
import string
import time
from typing import Iterator

import pytest
from dotenv import load_dotenv

from upstash_blob import Bucket

load_dotenv()

pytestmark = pytest.mark.live

RUN_ID = f"{int(time.time() * 1000):x}-{''.join(random.choices(string.ascii_lowercase, k=6))}"
SWEEP = "test/py/"
ROOT = f"{SWEEP}{RUN_ID}/"


def p(name: str) -> str:
    return ROOT + name


def _bucket(name: str) -> Bucket:
    if not os.environ.get(name):
        pytest.skip(f"{name} is not set")
    return Bucket.from_env(name)


def _sweep(bucket: Bucket) -> None:
    """A crashed run leaves test/py/<id>/ and multipart uploads behind; sweep anything over an hour old.

    Every SDK shares these buckets under test/<sdk>/, so touch nothing outside test/py/.
    """
    bucket.abort_stale_multipart_uploads(older_than="1h", prefix=SWEEP)
    cutoff = (time.time() - 3600) * 1000
    stale = set()
    cursor = None
    while True:
        page = bucket.list(prefix=SWEEP, limit=1000, cursor=cursor)
        for b in page.blobs:
            run = b.path[len(SWEEP) :].split("/")[0]
            try:
                started = int(run.split("-")[0], 16)
            except ValueError:
                continue
            if started < cutoff:
                stale.add(f"{SWEEP}{run}/")
        cursor = page.cursor
        if not cursor:
            break
    for prefix in stale:
        bucket.delete(prefix=prefix)


@pytest.fixture(scope="session")
def pub() -> Iterator[Bucket]:
    b = _bucket("UPSTASH_BLOB_PUBLIC_TOKEN")
    _sweep(b)
    yield b
    b.delete(prefix=ROOT)
    b.close()


@pytest.fixture(scope="session")
def priv() -> Iterator[Bucket]:
    b = _bucket("UPSTASH_BLOB_PRIVATE_TOKEN")
    _sweep(b)
    yield b
    b.delete(prefix=ROOT)
    b.close()
