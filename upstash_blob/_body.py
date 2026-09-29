import asyncio
import inspect
import mimetypes
import os
from dataclasses import dataclass
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Iterable,
    Iterator,
    Optional,
    Tuple,
    Union,
)

from upstash_blob._sniff import SNIFF_BYTES
from upstash_blob.errors import BlobError

PutBody = Union[bytes, bytearray, memoryview, str, "os.PathLike[str]", Any, Iterable[bytes]]
"""
bytes, a str (UTF-8), a ``pathlib.Path``, a binary file object, or an iterable of bytes chunks.
``AsyncBucket.put`` also takes an async iterable of bytes.
"""

CHUNK = 64 * 1024

Data = Union[bytes, Iterator[bytes]]
AsyncData = Union[bytes, AsyncIterator[bytes]]


@dataclass
class Resolved:
    data: Any
    """bytes, or an iterator (async iterator on the async side) of bytes chunks."""
    size: Optional[int]
    """None when the length is unknown up front."""
    content_type: Optional[str]
    close: Optional[Callable[[], None]] = None


def _guess(name: Any) -> Optional[str]:
    if isinstance(name, (str, os.PathLike)):
        return mimetypes.guess_type(os.fspath(name))[0]
    return None


def _as_bytes(chunk: Any) -> bytes:
    if isinstance(chunk, bytes):
        return chunk
    if isinstance(chunk, (bytearray, memoryview)):
        return bytes(chunk)
    if isinstance(chunk, str):
        raise TypeError("put() body yielded str; open files in binary mode ('rb') or encode the text")
    raise TypeError(f"put() body yielded {type(chunk).__name__}, expected bytes")


def _file_size(f: Any) -> Optional[int]:
    """What is left to read from the current position, when the file can say without reading it."""
    try:
        if not f.seekable():
            return None
        pos = f.tell()
        end = f.seek(0, os.SEEK_END)
        f.seek(pos)
    except (AttributeError, OSError, ValueError):
        return None
    if not isinstance(pos, int) or not isinstance(end, int):
        return None
    return max(0, end - pos)


def _read_file(f: Any) -> Iterator[bytes]:
    while True:
        chunk = _as_bytes(f.read(CHUNK))
        if not chunk:
            return
        yield chunk


def _is_file(body: Any) -> bool:
    return hasattr(body, "read") and callable(body.read)


def _reads_chunks(body: Any) -> bool:
    """read(n) works. aiohttp's BodyPartReader has a read() that takes no size, and iterates instead."""
    try:
        inspect.signature(body.read).bind(CHUNK)
    except TypeError:
        return False
    except ValueError:
        return True
    return True


def _iter_checked(body: Iterable[Any]) -> Iterator[bytes]:
    for chunk in body:
        b = _as_bytes(chunk)
        if b:
            yield b


def _resolve_common(body: Any) -> Optional[Resolved]:
    if isinstance(body, str):
        raw = body.encode()
        return Resolved(raw, len(raw), None)
    if isinstance(body, (bytes, bytearray, memoryview)):
        raw = bytes(body)
        return Resolved(raw, len(raw), None)
    if isinstance(body, os.PathLike):
        f = open(body, "rb")
        return Resolved(_read_file(f), _file_size(f), _guess(body), f.close)
    return None


def resolve(body: Any) -> Resolved:
    common = _resolve_common(body)
    if common is not None:
        return common
    if _is_file(body):
        return Resolved(_read_file(body), _file_size(body), _guess(getattr(body, "name", None)))
    if isinstance(body, Iterable):
        return Resolved(_iter_checked(body), None, None)
    raise TypeError("put() body must be bytes, str, a pathlib.Path, a binary file object or an iterable of bytes")


async def _aread_file(f: Any) -> AsyncIterator[bytes]:
    # A sync file is read on a worker thread so the event loop is never blocked on disk.
    sync = not inspect.iscoroutinefunction(f.read)
    while True:
        chunk = await asyncio.to_thread(f.read, CHUNK) if sync else f.read(CHUNK)
        if inspect.isawaitable(chunk):
            chunk = await chunk
        b = _as_bytes(chunk)
        if not b:
            return
        yield b


async def _aiter_sync(it: Iterator[bytes]) -> AsyncIterator[bytes]:
    for chunk in it:
        yield chunk


async def _aiter_checked(body: AsyncIterable[Any]) -> AsyncIterator[bytes]:
    async for chunk in body:
        b = _as_bytes(chunk)
        if b:
            yield b


def aresolve(body: Any) -> Resolved:
    if isinstance(body, os.PathLike):
        f = open(body, "rb")
        return Resolved(_aread_file(f), _file_size(f), _guess(body), f.close)
    common = _resolve_common(body)
    if common is not None:
        return common
    # Before __aiter__: an aiofiles file is async-iterable too, but line by line.
    if _is_file(body) and _reads_chunks(body):
        # An async file object (aiofiles) cannot say its size without awaiting; pass size= for it.
        size = None if inspect.iscoroutinefunction(body.read) else _file_size(body)
        return Resolved(_aread_file(body), size, _guess(getattr(body, "name", None)))
    if hasattr(body, "__aiter__"):
        return Resolved(_aiter_checked(body), None, None)
    if isinstance(body, Iterable):
        return Resolved(_aiter_sync(_iter_checked(body)), None, None)
    raise TypeError(
        "put() body must be bytes, str, a pathlib.Path, a binary file object, or a sync or async iterable of bytes"
    )


def _too_large(max_size: int) -> BlobError:
    return BlobError("too_large", f"body exceeds max_size ({max_size} bytes)")


def _mismatch(sent: int, size: int) -> BlobError:
    return BlobError("invalid_input", f"body was {sent}{'+' if sent > size else ''} bytes, {size} were declared")


# ------------------------------------------------------------------ sync


def read_all(data: Data, max_size: int) -> bytes:
    if isinstance(data, bytes):
        if len(data) > max_size:
            raise _too_large(max_size)
        return data
    out = bytearray()
    for chunk in data:
        out += chunk
        if len(out) > max_size:
            raise _too_large(max_size)
    return bytes(out)


def peek(data: Data) -> Tuple[bytes, Data]:
    """Reads up to SNIFF_BYTES, then hands back data that replays them ahead of the rest."""
    if isinstance(data, bytes):
        return data[:SNIFF_BYTES], data
    head = bytearray()
    it = data
    for chunk in it:
        head += chunk
        if len(head) >= SNIFF_BYTES:
            break
    buffered = bytes(head)

    def replay() -> Iterator[bytes]:
        if buffered:
            yield buffered
        yield from it

    return buffered[:SNIFF_BYTES], replay()


def limit(data: Data, max_size: int) -> Data:
    """Counts bytes through; over max_size the iterator raises so the PUT never completes."""
    if isinstance(data, bytes):
        if len(data) > max_size:
            raise _too_large(max_size)
        return data

    def counted(it: Iterator[bytes]) -> Iterator[bytes]:
        seen = 0
        for chunk in it:
            seen += len(chunk)
            if seen > max_size:
                raise _too_large(max_size)
            yield chunk

    return counted(data)


def exact(data: Data, size: int) -> Data:
    """A streamed body that ends short or runs long is the caller's mistake, named before httpx sees it."""
    if isinstance(data, bytes):
        return data

    def counted(it: Iterator[bytes]) -> Iterator[bytes]:
        sent = 0
        for chunk in it:
            sent += len(chunk)
            if sent > size:
                raise _mismatch(sent, size)
            yield chunk
        if sent != size:
            raise _mismatch(sent, size)

    return counted(data)


def parts_of(data: Data, part_size: int) -> Iterator[bytes]:
    """Exactly part_size bytes per part, the last one whatever is left."""
    if isinstance(data, bytes):
        view = memoryview(data)
        for i in range(0, len(data), part_size):
            yield bytes(view[i : i + part_size])
        return
    buf = bytearray()
    for chunk in data:
        buf += chunk
        while len(buf) >= part_size:
            yield bytes(buf[:part_size])
            del buf[:part_size]
    if buf:
        yield bytes(buf)


# ----------------------------------------------------------------- async


async def aread_all(data: AsyncData, max_size: int) -> bytes:
    if isinstance(data, bytes):
        return read_all(data, max_size)
    out = bytearray()
    async for chunk in data:
        out += chunk
        if len(out) > max_size:
            raise _too_large(max_size)
    return bytes(out)


async def apeek(data: AsyncData) -> Tuple[bytes, AsyncData]:
    if isinstance(data, bytes):
        return data[:SNIFF_BYTES], data
    head = bytearray()
    it = data
    async for chunk in it:
        head += chunk
        if len(head) >= SNIFF_BYTES:
            break
    buffered = bytes(head)

    async def replay() -> AsyncIterator[bytes]:
        if buffered:
            yield buffered
        async for chunk in it:
            yield chunk

    return buffered[:SNIFF_BYTES], replay()


def alimit(data: AsyncData, max_size: int) -> AsyncData:
    if isinstance(data, bytes):
        if len(data) > max_size:
            raise _too_large(max_size)
        return data

    async def counted(it: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
        seen = 0
        async for chunk in it:
            seen += len(chunk)
            if seen > max_size:
                raise _too_large(max_size)
            yield chunk

    return counted(data)


def aexact(data: AsyncData, size: int) -> AsyncData:
    if isinstance(data, bytes):
        return data

    async def counted(it: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
        sent = 0
        async for chunk in it:
            sent += len(chunk)
            if sent > size:
                raise _mismatch(sent, size)
            yield chunk
        if sent != size:
            raise _mismatch(sent, size)

    return counted(data)


async def aparts_of(data: AsyncData, part_size: int) -> AsyncIterator[bytes]:
    if isinstance(data, bytes):
        for part in parts_of(data, part_size):
            yield part
        return
    buf = bytearray()
    async for chunk in data:
        buf += chunk
        while len(buf) >= part_size:
            yield bytes(buf[:part_size])
            del buf[:part_size]
    if buf:
        yield bytes(buf)
