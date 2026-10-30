"""
providers/registry.py — Provider name -> instance mapping.

Nothing outside providers/ may reference provider-specific classes directly.
All callers resolve a provider by name through this registry.

The registry is a module-level singleton populated at startup.  In tests,
call register() with a fresh instance to inject a controlled mock.
"""

from __future__ import annotations

from smartdialer.providers.base import TelecomProvider

_registry: dict[str, TelecomProvider] = {}


def register(provider: TelecomProvider) -> None:
    """Register a provider instance under its name."""
    _registry[provider.name] = provider


def get(name: str) -> TelecomProvider:
    """
    Retrieve a provider by name.

    Raises KeyError if name is not registered.  Callers should catch this
    and treat it as a configuration error (not a runtime error).
    """
    if name not in _registry:
        raise KeyError(
            f"Provider '{name}' is not registered. "
            f"Known providers: {list(_registry.keys())}"
        )
    return _registry[name]


def all_providers() -> list[TelecomProvider]:
    """Return all registered providers (used by the sweeper and health monitor)."""
    return list(_registry.values())


def clear() -> None:
    """Remove all registrations.  Used in tests to reset state between runs."""
    _registry.clear()


def _register_defaults() -> None:
    """Register the default mock providers.  Called at module import time."""
    from smartdialer.providers.mock_a import MockProviderA
    from smartdialer.providers.mock_b import MockProviderB

    register(MockProviderA())
    register(MockProviderB())


# Register defaults when the module is imported.  Tests may call clear()
# then register() to replace with controlled instances.
_register_defaults()
