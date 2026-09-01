"""Entrypoint.

Run with: uvicorn main:app --reload
Requires a running Redis instance reachable at $REDIS_URL
(defaults to redis://localhost:6379/0).
"""
from broker.api import app  # noqa: F401
