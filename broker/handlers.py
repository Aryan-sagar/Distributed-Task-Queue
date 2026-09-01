from __future__ import annotations

import time
from typing import Any, Dict

from .registry import register


@register("noop")
def handle_noop(payload: Dict[str, Any]) -> None:
    return None


@register("sleep")
def handle_sleep(payload: Dict[str, Any]) -> None:
    time.sleep(payload.get("seconds", 0))


@register("send_email")
def handle_send_email(payload: Dict[str, Any]) -> str:
    # Placeholder — a real system would call an email provider here.
    return f"sent to {payload.get('to', 'unknown')}"
