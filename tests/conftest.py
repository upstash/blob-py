from typing import Iterator

import httpx
import pytest

from tests.fake import TOKEN, Fake
from upstash_blob import AsyncBucket, Bucket
from upstash_blob._core import reset_credential_caches


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    # Backoffs are real seconds; the behaviour under test is how many times, not how long.
    monkeypatch.setattr("upstash_blob._sync.time.sleep", lambda s: None)

    async def instant(s: float) -> None:
        return None

    monkeypatch.setattr("upstash_blob._async.asyncio.sleep", instant)


@pytest.fixture
def fake() -> Iterator[Fake]:
    reset_credential_caches()
    yield Fake()
    reset_credential_caches()


@pytest.fixture
def bucket(fake: Fake) -> Iterator[Bucket]:
    with Bucket(TOKEN, http_client=httpx.Client(transport=httpx.MockTransport(fake.handler))) as b:
        yield b


@pytest.fixture
def abucket(fake: Fake) -> AsyncBucket:
    return AsyncBucket(TOKEN, http_client=httpx.AsyncClient(transport=httpx.MockTransport(fake.ahandler)))
