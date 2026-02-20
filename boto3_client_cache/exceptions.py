"""Custom exceptions and warning types for boto3-client-cache."""

__all__ = [
    "CacheError",
    "ClientCacheError",
    "ClientCacheExistsError",
    "ClientCacheNotFoundError",
    "ResourceCacheError",
    "ResourceCacheExistsError",
    "ResourceCacheNotFoundError",
]

from typing import Any, Dict


class CacheError(Exception):
    """The base exception for boto3-client-cache.

    .. versionadded:: 2.0.0

    Parameters
    ----------
    message : str, optional
        The message to raise.
    code : str | int, optional
        A short machine-friendly code for the error.
    status_code : int, optional
        An HTTP status code, if applicable.
    details : Dict[str, Any], optional
        Extra structured metadata for debugging or logging.
    param : str, optional
        The parameter name related to the error.
    value : Any, optional
        The parameter value related to the error.
    """

    def __init__(
        self,
        message: str | None = None,
        *,
        code: str | int | None = None,
        status_code: int | None = None,
        details: Dict[str, Any] | None = None,
        param: str | None = None,
        value: Any | None = None,
    ) -> None:
        self.message = "" if message is None else message
        self.code = code
        self.status_code = status_code
        self.details = details
        self.param = param
        self.value = value
        super().__init__(self.message)

    def __str__(self) -> str:
        base = self.message
        extras: list[str] = []
        if self.code is not None:
            extras.append(f"code={self.code!r}")
        if self.status_code is not None:
            extras.append(f"status_code={self.status_code!r}")
        if self.param is not None:
            extras.append(f"param={self.param!r}")
        if self.value is not None:
            extras.append(f"value={self.value!r}")
        if self.details is not None:
            extras.append(f"details={self.details!r}")
        if extras:
            if base:
                return f"{base} ({', '.join(extras)})"
            return ", ".join(extras)
        return base

    def __repr__(self) -> str:
        args = [repr(self.message)]
        if self.code is not None:
            args.append(f"code={self.code!r}")
        if self.status_code is not None:
            args.append(f"status_code={self.status_code!r}")
        if self.param is not None:
            args.append(f"param={self.param!r}")
        if self.value is not None:
            args.append(f"value={self.value!r}")
        if self.details is not None:
            args.append(f"details={self.details!r}")
        return f"{self.__class__.__name__}({', '.join(args)})"


class ClientCacheError(CacheError):
    """The base exception for boto3-client-cache client cache errors.

    .. versionadded:: 0.1.0
    """


class ClientCacheExistsError(ClientCacheError):
    """Raised when attempting to add a client to the cache that already
    exists.

    .. versionadded:: 0.1.0
    """


class ClientCacheNotFoundError(ClientCacheError):
    """Raised when a client is not found in the cache.

    .. versionadded:: 0.1.0
    """


class ResourceCacheError(CacheError):
    """The base exception for boto3-client-cache resource cache errors.

    .. versionadded:: 2.0.0
    """


class ResourceCacheExistsError(ResourceCacheError):
    """Raised when attempting to add a resource to the cache that already
    exists.

    .. versionadded:: 2.0.0
    """


class ResourceCacheNotFoundError(ResourceCacheError):
    """Raised when a resource is not found in the cache.

    .. versionadded:: 2.0.0
    """
