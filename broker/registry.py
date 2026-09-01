from __future__ import annotations

from typing import Any, Callable, Dict, Optional

HandlerFn = Callable[[Dict[str, Any]], Any]

_REGISTRY: Dict[str, HandlerFn] = {}


def register(task_type: str) -> Callable[[HandlerFn], HandlerFn]:
    """Decorator to register a function as the handler for a task_type."""

    def decorator(fn: HandlerFn) -> HandlerFn:
        if task_type in _REGISTRY:
            raise ValueError(f"Handler already registered for task_type={task_type!r}")
        _REGISTRY[task_type] = fn
        return fn

    return decorator


def get_handler(task_type: str) -> Optional[HandlerFn]:
    return _REGISTRY.get(task_type)


def clear_registry() -> None:
    """Test helper — clears all registered handlers between test cases."""
    _REGISTRY.clear()
