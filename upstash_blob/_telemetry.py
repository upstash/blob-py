import os
from platform import python_version
from typing import Dict, Optional

from upstash_blob._version import __version__


def _disabled(value: Optional[str]) -> bool:
    # 'false' or '0' does not opt out: an env var that says false and means true is a trap.
    if value is None:
        return False
    v = value.strip().lower()
    return v not in ("", "false", "0", "no", "off")


def telemetry_headers() -> Dict[str, str]:
    """
    Sent only on calls to the Upstash agent (credentials and signing), not on object requests, which
    are the hot path. Opt out with ``UPSTASH_DISABLE_TELEMETRY`` or ``enable_telemetry=False``.
    """
    if _disabled(os.environ.get("UPSTASH_DISABLE_TELEMETRY")):
        return {}
    out = {
        "Upstash-Telemetry-Sdk": f"upstash-blob-py@v{__version__}",
        "Upstash-Telemetry-Runtime": f"python@v{python_version()}",
    }
    if os.environ.get("VERCEL"):
        out["Upstash-Telemetry-Platform"] = "vercel"
    elif os.environ.get("AWS_REGION"):
        out["Upstash-Telemetry-Platform"] = "aws"
    return out
