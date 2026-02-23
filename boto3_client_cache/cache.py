"""Low-level module for caching boto3 clients based on their initialization
parameters."""

from __future__ import annotations

__all__ = [
    "CacheType",
    "ClientCache",
    "ClientCacheKey",
    "EvictionPolicy",
    "LFUClientCache",
    "LFUResourceCache",
    "LRUClientCache",
    "LRUResourceCache",
    "ResourceCache",
    "ResourceCacheKey",
]

from abc import ABC, abstractmethod
from collections import OrderedDict
from collections.abc import Iterator
from inspect import signature
from threading import RLock
from typing import Any, Generic, Literal, Tuple, TypeVar, get_args

from boto3.resources.base import ServiceResource
from boto3.session import Session
from botocore.client import BaseClient
from botocore.config import Config

from .exceptions import (
    CacheError,
    ClientCacheError,
    ClientCacheExistsError,
    ClientCacheNotFoundError,
    ResourceCacheError,
    ResourceCacheExistsError,
    ResourceCacheNotFoundError,
)

#: Type alias for supported cache types
EvictionPolicy = Literal["LRU", "LFU"]

#: Type alias for cacheable objects in boto3-client-cache.
CacheType = Literal["client", "resource"]

_CacheObjType = TypeVar("_CacheObjType", BaseClient, ServiceResource)
_CacheKeyType = TypeVar("_CacheKeyType", "ClientCacheKey", "ResourceCacheKey")


class _AbstractCacheKey(ABC):
    """Abstract base class for cache keys.

    .. versionadded:: 2.0.0
    """

    def __init__(
        self, cache_type: CacheType | None = None, *args, **kwargs
    ) -> None:
        if cache_type is not None and (
            not isinstance(cache_type, str)
            or cache_type not in get_args(CacheType)
        ):
            raise CacheError(
                f"Invalid cache type '{cache_type}'. Supported types are: "
                f"{', '.join(get_args(CacheType))}."
            ) from None

        self.cache_type = cache_type or "client"

        # initializing the cache key and label
        self._create(*args, **kwargs)

    def __str__(self) -> str:
        return self.label

    def __repr__(self) -> str:
        value = (
            f"{self.cache_type.capitalize()}CacheKey"
            f"({self.cache_type}({self.label}))"
        )
        return value

    def __hash__(self) -> int:
        return hash(self._key)

    @abstractmethod
    def __eq__(self, other: object) -> bool: ...

    def _create(self, *args, **kwargs) -> None:
        """Creates the cache key and label based on the provided arguments.

        Parameters
        ----------
        *args : Any, optional
            Positional arguments used to create the cache key.
        **kwargs : Any, optional
            Keyword arguments used to create the cache key.
        """

        # immediately moving "service_name" into args if present in kwargs
        if "service_name" in kwargs:
            args = (kwargs.pop("service_name"),) + args

        # removing eviction_policy and max_size from kwargs if present
        if "eviction_policy" in kwargs:
            kwargs.pop("eviction_policy")
        if "max_size" in kwargs:
            kwargs.pop("max_size")

        # defining keys which may contain sensitive information to be obscured
        sensitive_keys = (
            "aws_access_key_id",
            "aws_secret_access_key",
            "aws_session_token",
        )

        # inferring positions of args which may contain sensitive info
        # based on the public function signature (excluding ``self``)
        method = (
            Session.client if self.cache_type == "client" else Session.resource
        )

        # filtering out "self" from the parameter names
        parameter_names = tuple(
            name for name in signature(method).parameters if name != "self"
        )

        # determining the positions of sensitive args based on keywords
        sensitive_arg_positions = tuple(
            i
            for i, name in enumerate(parameter_names)
            if name in sensitive_keys
        )

        # creating a private clear-text label
        self._label: str = ", ".join(
            [
                *(repr(a) for a in args),
                *(
                    f"{k}={self._format_label_value(v)}"
                    for k, v in sorted(kwargs.items())
                ),
            ]
        )

        # creating a public label with sensitive information obscured
        self.label: str = ", ".join(
            [
                *(
                    repr(a)
                    if i not in sensitive_arg_positions
                    else repr("***")
                    for i, a in enumerate(args)
                ),
                *(
                    f"{k}={self._format_label_value(v)}"
                    if k not in sensitive_keys
                    else f"{k}=***"
                    for k, v in sorted(kwargs.items())
                ),
            ]
        )

        # checking if Config was passed as a positional arg
        # also preemptively obscuring args which may contain sensitive info
        _args = [
            self._config_cache_key(arg)
            if isinstance(arg, Config)
            else "***"
            if i in sensitive_arg_positions
            else arg
            for i, arg in enumerate(args)
        ]

        # checking if Config was passed as a positional arg
        _clear_args = [
            self._config_cache_key(arg) if isinstance(arg, Config) else arg
            for arg in args
        ]

        # popping trailing None values from args, preserving None in middle
        while _args and _args[-1] is None:
            _args.pop()
        _args = tuple(_args)

        # popping trailing None values from _clear_args
        # but preserving None in middle
        while _clear_args and _clear_args[-1] is None:
            _clear_args.pop()
        _clear_args = tuple(_clear_args)

        # checking if Config was passed as a keyword arg
        _kwargs = kwargs.copy()
        if _kwargs.get("config") is not None:
            _kwargs["config"] = self._config_cache_key(_kwargs["config"])

        # checking if Config was passed as a keyword arg
        _clear_kwargs = kwargs.copy()
        if _clear_kwargs.get("config") is not None:
            _clear_kwargs["config"] = self._config_cache_key(
                _clear_kwargs["config"]
            )

        # preemptively removing None values from kwargs
        # also obscuring kwarg values which may contain sensitive info
        _kwargs = {
            key: value if key not in sensitive_keys else "***"
            for key, value in _kwargs.items()
            if value is not None
        }

        _clear_kwargs = {
            key: value
            for key, value in _clear_kwargs.items()
            if value is not None
        }

        # creating a public unique key for the client cache
        self.key = (_args, tuple(sorted(_kwargs.items())))

        # creating a private clear-text key for equality checks and hashing
        self._key = (_clear_args, tuple(sorted(_clear_kwargs.items())))

    def _freeze_value(self, value: Any) -> Any:
        """Recursively freezes a value for use in cache keys.

        Parameters
        ----------
        value : Any
            The value to freeze.

        Returns
        -------
        Any
            A hashable representation of the value.
        """

        match value:
            # recursively freezing dicts
            case dict():
                return tuple(
                    sorted(
                        (key, self._freeze_value(val))
                        for key, val in value.items()
                    )
                )

            # recursively freezing lists and tuples
            case list() | tuple():
                return tuple(self._freeze_value(item) for item in value)

            # recursively freezing sets
            case set():
                return tuple(
                    sorted(self._freeze_value(item) for item in value)
                )
            # everything else remains unchanged
            case _:
                return value

    def _config_cache_key(self, config: Config | None) -> Any:
        """Generates a cache key for a botocore.config.Config object.

        Parameters
        ----------
        config : Config | None
            The Config object to generate a cache key for.

        Returns
        -------
        Any
            A hashable representation of the Config object for use in cache
            keys.
        """

        if config is None:
            return None

        # checking for user-provided options first
        options = getattr(config, "_user_provided_options", None)
        if options is None:
            # __dict__ is pedantic but stable
            return self._freeze_value(getattr(config, "__dict__", {}))

        return self._freeze_value(options)

    def _format_label_value(self, value: Any) -> str:
        """Formats a value for use in cache key labels.

        Parameters
        ----------
        value : Any
            The value to format.

        Returns
        -------
        str
            The formatted string representation of the value.
        """

        if isinstance(value, Config):
            # checking for user-provided options first
            options = getattr(value, "_user_provided_options", None)

            # falling back to __dict__ if no user-provided options exist
            if options is None:
                options = getattr(value, "__dict__", {})

            # creating a sorted string representation of the options
            options = ", ".join(
                [f"{k}={v!r}" for k, v in sorted(options.items())]
            )
            return f"Config({options})"

        return repr(value)


class ClientCacheKey(_AbstractCacheKey):
    """A unique, hashable key for caching clients based on their
    initialization parameters.

    In order to interact with the cache, instances of this class should be
    created using the same arguments that would be used to initialize the
    boto3 client.

    .. versionadded:: 0.1.0

    Parameters
    ----------
    *args : Any, optional
        Positional arguments used to create the cache key.
    **kwargs : Any, optional
        Keyword arguments used to create the cache key.

    Attributes
    ----------
    key : tuple
        The unique key representing the client's initialization parameters.
    label : str
        A human-readable label for the cache key, useful for debugging.

    Examples
    --------
    Creating a cache key for an S3 client initialized with a specific region:

    >>> from boto3_client_cache import ClientCacheKey
    ...
    >>> key = ClientCacheKey("s3", region_name="us-west-2")
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__("client", *args, **kwargs)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, ClientCacheKey) and self._key == other._key

    # inheriting the hash implementation from _AbstractCacheKey
    __hash__ = _AbstractCacheKey.__hash__


class ResourceCacheKey(_AbstractCacheKey):
    """A unique, hashable key for caching resources based on their
    initialization parameters.

    In order to interact with the cache, instances of this class should be
    created using the same arguments that would be used to initialize the
    boto3 resource.

    .. versionadded:: 2.0.0

    Parameters
    ----------
    *args : Any, optional
        Positional arguments used to create the cache key.
    **kwargs : Any, optional
        Keyword arguments used to create the cache key.

    Attributes
    ----------
    key : tuple
        The unique key representing the resource's initialization parameters.
    label : str
        A human-readable label for the cache key, useful for debugging.

    Examples
    --------
    Creating a cache key for an S3 resource initialized with a specific region:

    >>> from boto3_client_cache import ResourceCacheKey
    ...
    >>> key = ResourceCacheKey("s3", region_name="us-west-2")
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__("resource", *args, **kwargs)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, ResourceCacheKey) and self._key == other._key

    # inheriting the hash implementation from _AbstractCacheKey
    __hash__ = _AbstractCacheKey.__hash__


class _AbstractCache(ABC, Generic[_CacheObjType, _CacheKeyType]):
    """Abstract base class for client and resource caches.

    .. versionadded:: 2.0.0
    """

    @abstractmethod
    def __str__(self) -> str: ...

    @abstractmethod
    def __repr__(self) -> str: ...

    @abstractmethod
    def __len__(self) -> int: ...

    @abstractmethod
    def __reversed__(self) -> Iterator[_CacheKeyType]: ...

    @abstractmethod
    def __contains__(self, key: _CacheKeyType) -> bool: ...

    @abstractmethod
    def __iter__(self) -> Iterator[_CacheKeyType]: ...

    @abstractmethod
    def __getitem__(self, key: _CacheKeyType) -> _CacheObjType: ...

    @abstractmethod
    def __setitem__(self, key: _CacheKeyType, obj: _CacheObjType) -> None: ...

    @abstractmethod
    def __delitem__(self, key: _CacheKeyType) -> None: ...

    @abstractmethod
    def keys(self) -> Tuple[_CacheKeyType, ...]: ...

    @abstractmethod
    def values(self) -> Tuple[_CacheObjType, ...]: ...

    @abstractmethod
    def items(
        self,
    ) -> Tuple[Tuple[_CacheKeyType, _CacheObjType], ...]: ...

    @abstractmethod
    def get(
        self, key: _CacheKeyType, default: _CacheObjType | None = None
    ) -> _CacheObjType | None: ...

    @abstractmethod
    def pop(self, key: _CacheKeyType) -> _CacheObjType: ...

    @abstractmethod
    def clear(self) -> None: ...

    @abstractmethod
    def popitem(self) -> Tuple[_CacheKeyType, _CacheObjType]: ...

    @abstractmethod
    def copy(self) -> "_AbstractCache": ...


class _ClientCacheRegistry:
    """Lightweight class-level registry for mapping ``ClientCache``
    to cache implementations.

    .. versionadded:: 0.1.0

    Attributes
    ----------
    registry
        The class-level registry mapping keys to classes.
    """

    registry = {}

    def __init_subclass__(
        cls, *, eviction_policy: EvictionPolicy, **kwargs: Any
    ) -> None:
        # calling the parent method to ensure proper subclass initialization
        super().__init_subclass__(**kwargs)

        # registering the subclass using the provided cache type
        cls.registry[eviction_policy] = cls


class _ResourceCacheRegistry:
    """Lightweight class-level registry for mapping ``ResourceCache``
    to cache implementations.

    .. versionadded:: 2.0.0

    Attributes
    ----------
    registry
        The class-level registry mapping keys to classes.
    """

    registry = {}

    def __init_subclass__(
        cls, *, eviction_policy: EvictionPolicy, **kwargs: Any
    ) -> None:
        # calling the parent method to ensure proper subclass initialization
        super().__init_subclass__(**kwargs)

        # registering the subclass using the provided cache type
        cls.registry[eviction_policy] = cls


class _BaseLRUCache(_AbstractCache, Generic[_CacheObjType, _CacheKeyType]):
    def __init__(
        self, cache_type: CacheType | None = None, max_size: int | None = None
    ) -> None:
        if cache_type is not None and cache_type not in get_args(CacheType):
            raise CacheError(
                f"Invalid cache type '{cache_type}'. Supported types are: "
                f"{', '.join(get_args(CacheType))}."
            ) from None

        self.cache_type = cache_type or "client"
        self._max_size = abs(max_size if max_size is not None else 10)
        self._cache: OrderedDict[_CacheKeyType, _CacheObjType] = OrderedDict()
        self._lock = RLock()

    @property
    def max_size(self) -> int:
        """The maximum number of clients to store in the cache."""

        return self._max_size

    @max_size.setter
    def max_size(self, value: int) -> None:
        """Sets the maximum size of the cache. If the new maximum size is less
        than the current number of items in the cache, the least recently used
        items will be evicted until the cache size is within the new limit.

        Parameters
        ----------
        value : int
            The new maximum size for the cache.
        """

        with self._lock:
            self._max_size = abs(value)
            while len(self._cache) > self._max_size:
                self._cache.popitem(last=False)

    def __str__(self) -> str:
        with self._lock:
            if not self._cache:
                return f"{self.cache_type.capitalize()}Cache(empty)"
            labels = "\n   ".join(
                f"- RefreshableSession.{self.cache_type}({key.label})"
                for key in self._cache.keys()
            )
            return f"{self.cache_type.capitalize()}Cache:\n   {labels}"

    def __repr__(self) -> str:
        return self.__str__()

    def __len__(self) -> int:
        with self._lock:
            return len(self._cache)

    def __reversed__(self) -> Iterator[_CacheKeyType]:
        with self._lock:
            return iter(tuple(reversed(self._cache.keys())))

    def __contains__(self, key: _CacheKeyType) -> bool:
        with self._lock:
            return key in self._cache

    def __iter__(self) -> Iterator[_CacheKeyType]:
        with self._lock:
            return iter(tuple(self._cache.keys()))

    def __getitem__(self, key: _CacheKeyType) -> _CacheObjType:
        with self._lock:
            # move obj to end of cache to mark it as recently used
            if key in self._cache:
                self._cache.move_to_end(key)
                return self._cache[key]
            else:
                msg = "The client you requested has not been cached."
                match self.cache_type:
                    case "client":
                        raise ClientCacheNotFoundError(msg) from None
                    case _:
                        raise ResourceCacheNotFoundError(msg) from None

    def __setitem__(self, key: _CacheKeyType, obj: _CacheObjType) -> None:
        if not isinstance(key, _AbstractCacheKey):
            msg = "Cache key must be a cache key object."

            match self.cache_type:
                case "client":
                    raise ClientCacheError(msg) from None
                case _:
                    raise ResourceCacheError(msg) from None

        if not isinstance(obj, (BaseClient, ServiceResource)):
            msg = (
                "Cache value must be a boto3 client or resource object, "
                f"{type(obj)} provided."
            )

            match self.cache_type:
                case "client":
                    raise ClientCacheError(msg) from None
                case _:
                    raise ResourceCacheError(msg) from None

        with self._lock:
            if key in self._cache:
                msg = "Client already exists in cache."

                match self.cache_type:
                    case "client":
                        raise ClientCacheExistsError(msg) from None
                    case _:
                        raise ResourceCacheExistsError(msg) from None

            # setting the object
            self._cache[key] = obj
            # marking the object as recently used
            self._cache.move_to_end(key)

            # removing least recently used object if cache exceeds max size
            if len(self._cache) > self.max_size:
                self._cache.popitem(last=False)

    def __delitem__(self, key: _CacheKeyType) -> None:
        with self._lock:
            if key not in self._cache:
                msg = "Client not found in cache."

                match self.cache_type:
                    case "client":
                        raise ClientCacheNotFoundError(msg) from None
                    case _:
                        raise ResourceCacheNotFoundError(msg) from None
            del self._cache[key]

    def keys(self) -> Tuple[_CacheKeyType, ...]:
        """Returns the keys in the cache."""

        with self._lock:
            return tuple(self._cache.keys())

    def values(self) -> Tuple[_CacheObjType, ...]:
        """Returns the values from the cache."""

        with self._lock:
            return tuple(self._cache.values())

    def items(self) -> Tuple[Tuple[_CacheKeyType, _CacheObjType], ...]:
        """Returns the items in the cache as (_CacheKeyType, _CacheObjType)
        tuples."""

        with self._lock:
            return tuple(self._cache.items())

    def get(
        self, key: _CacheKeyType, default: _CacheObjType | None = None
    ) -> _CacheObjType | None:
        """Gets the object using the given key, or returns the default."""

        with self._lock:
            # move obj to end of cache to mark it as recently used
            if key in self._cache:
                self._cache.move_to_end(key)
            return self._cache.get(key, default)

    def pop(self, key: _CacheKeyType) -> _CacheObjType:
        """Pops and returns the object associated with the given key."""

        with self._lock:
            if (obj := self._cache.get(key)) is None:
                msg = "Client not found in cache."

                match self.cache_type:
                    case "client":
                        raise ClientCacheNotFoundError(msg) from None
                    case _:
                        raise ResourceCacheNotFoundError(msg) from None

            del self._cache[key]
            return obj

    def clear(self) -> None:
        """Clears all items from the cache."""

        with self._lock:
            self._cache.clear()

    def popitem(self) -> Tuple[_CacheKeyType, _CacheObjType]:
        """Pops and returns the least recently used item from the cache."""

        with self._lock:
            if not self._cache:
                msg = "No clients found in cache."

                match self.cache_type:
                    case "client":
                        raise ClientCacheNotFoundError(msg) from None
                    case _:
                        raise ResourceCacheNotFoundError(msg) from None

            return self._cache.popitem(last=False)

    def copy(self) -> "_BaseLRUCache[_CacheObjType, _CacheKeyType]":
        """Returns a shallow copy of the cache."""

        with self._lock:
            new_cache = self.__class__(max_size=self.max_size)
            new_cache.cache_type = self.cache_type
            new_cache._cache = self._cache.copy()
            return new_cache


class LRUClientCache(
    _BaseLRUCache[BaseClient, ClientCacheKey],
    _ClientCacheRegistry,
    eviction_policy="LRU",
):
    """A thread-safe LRU cache for storing clients which can be used exactly
    like a dictionary.

    The cache has a maximum size attribute, and retrieved and newly added
    clients are marked as recently used. When the cache exceeds its maximum
    size, the least recently used client is evicted. When setting a client,
    use :class:`ClientCacheKey` for the key, not ``*args`` and ``**kwargs``.

    Editing the max size of the cache after initialization is supported, and
    will evict least recently used items until the cache size is within the
    new limit if the new maximum size is less than the current number of items
    in the cache.

    Attempting to overwrite an existing client in the cache will raise an
    error.

    ``LRUClientCache`` does not support ``fromkeys``, ``update``,
    ``setdefault``, the ``|=`` operator, or the ``|`` operator.

    .. versionadded:: 0.1.0

    Parameters
    ----------
    max_size : int, optional
        The maximum number of clients to store in the cache. Defaults to 10.

    Attributes
    ----------
    max_size : int, optional
        The maximum number of clients to store in the cache. Defaults to 10.

    Methods
    -------
    clear() -> None
        Clears all clients from the cache.
    copy() -> LRUClientCache
        Returns a shallow copy of the cache.
    get(key: ClientCacheKey, default: BaseClient = None) -> BaseClient | None
        Gets the client associated with the given key, or returns the default.
    items() -> Tuple[Tuple[ClientCacheKey, BaseClient], ...]
        Returns the items in the cache as (key, client) tuples.
    keys() -> Tuple[ClientCacheKey, ...]
        Returns the keys in the cache.
    pop(key: ClientCacheKey) -> BaseClient
        Pops and returns the client associated with the given key.
    popitem() -> Tuple[ClientCacheKey, BaseClient]
        Pops and returns the least recently used client as a (key, client)
        tuple.
    values() -> Tuple[BaseClient, ...]
        Returns the clients in the cache.

    Raises
    ------
    ClientCacheError
        Raised when an error occurs related to cache operations, such as using
        an invalid key or value type.
    ClientCacheExistsError
        Raised when attempting to add a client which already exists in the
        cache.
    ClientCacheNotFoundError
        Raised when attempting to retrieve or delete a client which does not
        exist in the cache.

    Examples
    --------
    >>> from boto3_client_cache import LRUClientCache, ClientCacheKey
    >>> cache = LRUClientCache(max_size=2)
    >>> kwargs = {"service_name": "s3", "region_name": "us-west-2"}
    >>> cache[ClientCacheKey(**kwargs)] = boto3.client(**kwargs)
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(cache_type="client", *args, **kwargs)


class LRUResourceCache(
    _BaseLRUCache[ServiceResource, ResourceCacheKey],
    _ResourceCacheRegistry,
    eviction_policy="LRU",
):
    """A thread-safe LRU cache for storing resources which can be used exactly
    like a dictionary.

    The cache has a maximum size attribute, and retrieved and newly added
    resources are marked as recently used. When the cache exceeds its maximum
    size, the least recently used resource is evicted. When setting a resource,
    use :class:`ResourceCacheKey` for the key, not ``*args`` and ``**kwargs``.

    Editing the max size of the cache after initialization is supported, and
    will evict least recently used items until the cache size is within the
    new limit if the new maximum size is less than the current number of items
    in the cache.

    Attempting to overwrite an existing resource in the cache will raise an
    error.

    ``LRUResourceCache`` does not support ``fromkeys``, ``update``,
    ``setdefault``, the ``|=`` operator, or the ``|`` operator.

    .. versionadded:: 2.0.0

    Parameters
    ----------
    max_size : int, optional
        The maximum number of resources to store in the cache. Defaults to 10.

    Attributes
    ----------
    max_size : int, optional
        The maximum number of resources to store in the cache. Defaults to 10.

    Methods
    -------
    clear() -> None
        Clears all resources from the cache.
    copy() -> LRUResourceCache
        Returns a shallow copy of the cache.
    get(
        key: ResourceCacheKey, default: ServiceResource = None
    ) -> ServiceResource | None
        Gets the resource associated with the given key, or returns the
        default.
    items() -> Tuple[Tuple[ResourceCacheKey, ServiceResource], ...]
        Returns the items in the cache as (key, resource) tuples.
    keys() -> Tuple[ResourceCacheKey, ...]
        Returns the keys in the cache.
    pop(key: ResourceCacheKey) -> ServiceResource
        Pops and returns the resource associated with the given key.
    popitem() -> Tuple[ResourceCacheKey, ServiceResource]
        Pops and returns the least recently used resource as a (key, resource)
        tuple.
    values() -> Tuple[ServiceResource, ...]
        Returns the resources in the cache.

    Raises
    ------
    ResourceCacheError
        Raised when an error occurs related to cache operations, such as using
        an invalid key or value type.
    ResourceCacheExistsError
        Raised when attempting to add a resource which already exists in the
        cache.
    ResourceCacheNotFoundError
        Raised when attempting to retrieve or delete a resource which does not
        exist in the cache.

    Examples
    --------
    >>> from boto3_client_cache import LRUResourceCache, ResourceCacheKey
    >>> cache = LRUResourceCache(max_size=2)
    >>> kwargs = {"service_name": "s3", "region_name": "us-west-2"}
    >>> cache[ResourceCacheKey(**kwargs)] = boto3.client(**kwargs)
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(cache_type="resource", *args, **kwargs)


class _FrequencyNode(Generic[_CacheKeyType]):
    """Internal node for a specific frequency in the LFU index."""

    def __init__(self, frequency: int) -> None:
        self.frequency = frequency
        self.keys: OrderedDict[_CacheKeyType, None] = OrderedDict()
        self.prev: "_FrequencyNode[_CacheKeyType] | None" = None
        self.next: "_FrequencyNode[_CacheKeyType] | None" = None


class _FrequencyIndex(Generic[_CacheKeyType]):
    """Internal O(1) index for LFU key ordering.

    Frequencies are stored in an ordered linked list. Each frequency node
    stores keys in an ``OrderedDict`` to preserve LRU tie-breaking among keys
    with the same access frequency.
    """

    def __init__(self) -> None:
        self._head: _FrequencyNode[_CacheKeyType] | None = None
        self._tail: _FrequencyNode[_CacheKeyType] | None = None
        self._key_to_node: dict[
            _CacheKeyType, _FrequencyNode[_CacheKeyType]
        ] = {}

    def insert(self, key: _CacheKeyType) -> None:
        if self._head is None or self._head.frequency != 1:
            node = _FrequencyNode[_CacheKeyType](1)
            self._insert_before(self._head, node)
        else:
            node = self._head

        node.keys[key] = None
        self._key_to_node[key] = node

    def increment(self, key: _CacheKeyType) -> None:
        current = self._key_to_node[key]
        target_frequency = current.frequency + 1
        target = current.next

        if target is None or target.frequency != target_frequency:
            target = _FrequencyNode[_CacheKeyType](target_frequency)
            self._insert_after(current, target)

        del current.keys[key]
        target.keys[key] = None
        self._key_to_node[key] = target
        self._prune(current)

    def delete(self, key: _CacheKeyType) -> None:
        node = self._key_to_node.pop(key)
        del node.keys[key]
        self._prune(node)

    def pop_least_frequent(self) -> _CacheKeyType:
        if self._head is None:
            raise CacheError("No keys are available in the LFU index.")

        node = self._head
        key, _ = node.keys.popitem(last=False)
        del self._key_to_node[key]
        self._prune(node)
        return key

    def iter_keys(self) -> Iterator[_CacheKeyType]:
        current = self._head
        while current is not None:
            yield from current.keys
            current = current.next

    def iter_keys_reversed(self) -> Iterator[_CacheKeyType]:
        current = self._tail
        while current is not None:
            yield from reversed(current.keys)
            current = current.prev

    def copy(self) -> "_FrequencyIndex[_CacheKeyType]":
        clone = _FrequencyIndex[_CacheKeyType]()
        current = self._head
        previous_clone_node: _FrequencyNode[_CacheKeyType] | None = None

        while current is not None:
            clone_node = _FrequencyNode[_CacheKeyType](current.frequency)
            clone_node.keys = current.keys.copy()

            if clone._head is None:
                clone._head = clone_node
            if previous_clone_node is not None:
                previous_clone_node.next = clone_node
                clone_node.prev = previous_clone_node

            for key in clone_node.keys:
                clone._key_to_node[key] = clone_node

            previous_clone_node = clone_node
            current = current.next

        clone._tail = previous_clone_node
        return clone

    def _insert_before(
        self,
        reference: _FrequencyNode[_CacheKeyType] | None,
        node: _FrequencyNode[_CacheKeyType],
    ) -> None:
        if reference is None:
            if self._tail is None:
                self._head = node
            else:
                self._tail.next = node
                node.prev = self._tail
            self._tail = node
            return

        previous = reference.prev
        node.prev = previous
        node.next = reference
        reference.prev = node

        if previous is None:
            self._head = node
        else:
            previous.next = node

    def _insert_after(
        self,
        reference: _FrequencyNode[_CacheKeyType],
        node: _FrequencyNode[_CacheKeyType],
    ) -> None:
        next_node = reference.next
        reference.next = node
        node.prev = reference
        node.next = next_node

        if next_node is None:
            self._tail = node
        else:
            next_node.prev = node

    def _prune(self, node: _FrequencyNode[_CacheKeyType]) -> None:
        if node.keys:
            return

        previous = node.prev
        next_node = node.next

        if previous is None:
            self._head = next_node
        else:
            previous.next = next_node

        if next_node is None:
            self._tail = previous
        else:
            next_node.prev = previous


class _BaseLFUCache(
    _AbstractCache,
    Generic[_CacheObjType, _CacheKeyType],
):
    def __init__(
        self, cache_type: CacheType | None = None, max_size: int | None = None
    ) -> None:
        if cache_type is not None and cache_type not in get_args(CacheType):
            raise CacheError(
                f"Invalid cache type '{cache_type}'. Supported types are: "
                f"{', '.join(get_args(CacheType))}."
            ) from None

        self.cache_type = cache_type or "client"
        self._max_size = abs(max_size if max_size is not None else 10)
        self._cache: dict[_CacheKeyType, _CacheObjType] = {}
        self._frequencies: _FrequencyIndex[_CacheKeyType] = _FrequencyIndex()
        self._lock = RLock()

    @property
    def max_size(self) -> int:
        """The maximum number of clients to store in the cache."""

        return self._max_size

    @max_size.setter
    def max_size(self, value: int) -> None:
        """Sets the maximum size of the cache.

        If the new maximum size is less than the current number of items in
        the cache, least frequently used clients are evicted until the cache
        size is within the new limit.
        """

        with self._lock:
            self._max_size = abs(value)
            while len(self._cache) > self._max_size:
                del self._cache[self._frequencies.pop_least_frequent()]

    def __str__(self) -> str:
        with self._lock:
            keys = tuple(self._frequencies.iter_keys())
            if not keys:
                return f"{self.cache_type.capitalize()}Cache(empty)"
            labels = "\n   ".join(
                f"- RefreshableSession.{self.cache_type}({key.label})"
                for key in keys
            )
            return f"{self.cache_type.capitalize()}Cache:\n   {labels}"

    def __repr__(self) -> str:
        return self.__str__()

    def __len__(self) -> int:
        with self._lock:
            return len(self._cache)

    def __reversed__(self) -> Iterator[_CacheKeyType]:
        with self._lock:
            return iter(tuple(self._frequencies.iter_keys_reversed()))

    def __contains__(self, key: _CacheKeyType) -> bool:
        with self._lock:
            return key in self._cache

    def __iter__(self) -> Iterator[_CacheKeyType]:
        with self._lock:
            return iter(tuple(self._frequencies.iter_keys()))

    def __getitem__(self, key: _CacheKeyType) -> _CacheObjType:
        with self._lock:
            if key not in self._cache:
                msg = "The client you requested has not been cached."

                match self.cache_type:
                    case "client":
                        raise ClientCacheNotFoundError(msg) from None
                    case _:
                        raise ResourceCacheNotFoundError(msg) from None

            self._frequencies.increment(key)
            return self._cache[key]

    def __setitem__(self, key: _CacheKeyType, obj: _CacheObjType) -> None:
        if not isinstance(key, _AbstractCacheKey):
            msg = (
                f"Cache key must be of type '{self.cache_type.capitalize()}"
                "CacheKey'."
            )

            match self.cache_type:
                case "client":
                    raise ClientCacheError(msg) from None
                case _:
                    raise ResourceCacheError(msg) from None

        if not isinstance(obj, (BaseClient, ServiceResource)):
            msg = (
                "Cache value must be a boto3 client or resource object, "
                f"{type(obj)} provided."
            )

            match self.cache_type:
                case "client":
                    raise ClientCacheError(msg) from None
                case _:
                    raise ResourceCacheError(msg) from None

        with self._lock:
            if key in self._cache:
                msg = "Client already exists in cache."

                match self.cache_type:
                    case "client":
                        raise ClientCacheExistsError(msg) from None
                    case _:
                        raise ResourceCacheExistsError(msg) from None

            if self.max_size == 0:
                return

            if len(self._cache) >= self.max_size:
                evicted_key = self._frequencies.pop_least_frequent()
                del self._cache[evicted_key]

            self._cache[key] = obj
            self._frequencies.insert(key)

    def __delitem__(self, key: _CacheKeyType) -> None:
        with self._lock:
            if key not in self._cache:
                msg = "Client not found in cache."

                match self.cache_type:
                    case "client":
                        raise ClientCacheNotFoundError(msg) from None
                    case _:
                        raise ResourceCacheNotFoundError(msg) from None

            self._frequencies.delete(key)
            del self._cache[key]

    def keys(self) -> Tuple[_CacheKeyType, ...]:
        """Returns the keys in LFU order.

        Keys are ordered by increasing frequency. Within each frequency bucket,
        keys are ordered least-recently used to most-recently used.
        """

        with self._lock:
            return tuple(self._frequencies.iter_keys())

    def values(self) -> Tuple[_CacheObjType, ...]:
        """Returns the values in LFU order."""

        with self._lock:
            keys = tuple(self._frequencies.iter_keys())
            return tuple(self._cache[key] for key in keys)

    def items(self) -> Tuple[Tuple[_CacheKeyType, _CacheObjType], ...]:
        """Returns the items in LFU order."""

        with self._lock:
            keys = tuple(self._frequencies.iter_keys())
            return tuple((key, self._cache[key]) for key in keys)

    def get(
        self, key: _CacheKeyType, default: _CacheObjType | None = None
    ) -> _CacheObjType | None:
        """Gets the object using the given key, or returns the default."""

        with self._lock:
            if key not in self._cache:
                return default
            self._frequencies.increment(key)
            return self._cache[key]

    def pop(self, key: _CacheKeyType) -> _CacheObjType:
        """Pops and returns the object associated with the given key."""

        with self._lock:
            obj = self._cache.get(key)
            if obj is None:
                msg = "Client not found in cache."

                match self.cache_type:
                    case "client":
                        raise ClientCacheNotFoundError(msg) from None
                    case _:
                        raise ResourceCacheNotFoundError(msg) from None

            self._frequencies.delete(key)
            del self._cache[key]
            return obj

    def clear(self) -> None:
        """Clears all items from the cache."""

        with self._lock:
            self._cache.clear()
            self._frequencies = _FrequencyIndex()

    def popitem(self) -> Tuple[_CacheKeyType, _CacheObjType]:
        """Pops and returns the LFU item from the cache."""

        with self._lock:
            if not self._cache:
                msg = "No clients found in cache."

                match self.cache_type:
                    case "client":
                        raise ClientCacheNotFoundError(msg) from None
                    case _:
                        raise ResourceCacheNotFoundError(msg) from None

            key = self._frequencies.pop_least_frequent()
            return key, self._cache.pop(key)

    def copy(self) -> "_BaseLFUCache":
        """Returns a shallow copy of the cache."""

        with self._lock:
            new_cache = self.__class__(max_size=self.max_size)
            new_cache.cache_type = self.cache_type
            new_cache._cache = self._cache.copy()
            new_cache._frequencies = self._frequencies.copy()
            return new_cache


class LFUClientCache(
    _BaseLFUCache[BaseClient, ClientCacheKey],
    _ClientCacheRegistry,
    eviction_policy="LFU",
):
    """A thread-safe LFU cache [1]_ for storing clients which can be used
    exactly like a dictionary.

    The cache has a maximum size attribute, and retrieved clients are promoted
    to a higher frequency bucket. When the cache exceeds its maximum size, the
    least frequently used client is evicted. If multiple clients share the same
    frequency, the least recently used client within that frequency bucket is
    evicted first. When setting a client, use :class:`ClientCacheKey` for the
    key, not ``*args`` and ``**kwargs``.

    Editing the max size of the cache after initialization is supported, and
    will evict least frequently used items until the cache size is within the
    new limit if the new maximum size is less than the current number of items
    in the cache.

    Attempting to overwrite an existing client in the cache will raise an
    error.

    ``LFUClientCache`` does not support ``fromkeys``, ``update``,
    ``setdefault``, the ``|=`` operator, or the ``|`` operator.

    .. versionadded:: 1.0.0

    Parameters
    ----------
    max_size : int, optional
        The maximum number of clients to store in the cache. Defaults to 10.

    Attributes
    ----------
    max_size : int, optional
        The maximum number of clients to store in the cache. Defaults to 10.

    Methods
    -------
    clear() -> None
        Clears all clients from the cache.
    copy() -> LFUClientCache
        Returns a shallow copy of the cache.
    get(key: ClientCacheKey, default: BaseClient = None) -> BaseClient | None
        Gets the client associated with the given key, or returns the default.
    items() -> Tuple[Tuple[ClientCacheKey, BaseClient], ...]
        Returns the items in the cache as (key, client) tuples.
    keys() -> Tuple[ClientCacheKey, ...]
        Returns the keys in the cache.
    pop(key: ClientCacheKey) -> BaseClient
        Pops and returns the client associated with the given key.
    popitem() -> Tuple[ClientCacheKey, BaseClient]
        Pops and returns the least frequently used client as a (key, client)
        tuple.
    values() -> Tuple[BaseClient, ...]
        Returns the clients in the cache.

    Raises
    ------
    ClientCacheError
        Raised when an error occurs related to cache operations, such as using
        an invalid key or value type.
    ClientCacheExistsError
        Raised when attempting to add a client which already exists in the
        cache.
    ClientCacheNotFoundError
        Raised when attempting to retrieve or delete a client which does not
        exist in the cache.

    References
    ----------
    .. [1] "An O(1) algorithm for implementing the LFU cache eviction scheme",
       http://dhruvbird.com/lfu.pdf

    Examples
    --------
    >>> from boto3_client_cache import LFUClientCache, ClientCacheKey
    >>> cache = LFUClientCache(max_size=2)
    >>> kwargs = {"service_name": "s3", "region_name": "us-west-2"}
    >>> cache[ClientCacheKey(**kwargs)] = boto3.client(**kwargs)
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(cache_type="client", *args, **kwargs)


class LFUResourceCache(
    _BaseLFUCache[ServiceResource, ResourceCacheKey],
    _ResourceCacheRegistry,
    eviction_policy="LFU",
):
    """A thread-safe LFU cache [1]_ for storing resources which can be used
    exactly like a dictionary.

    The cache has a maximum size attribute, and retrieved resources are
    promoted to a higher frequency bucket. When the cache exceeds its maximum
    size, the least frequently used resource is evicted. If multiple resources
    share the same frequency, the least recently used resource within that
    frequency bucket is evicted first. When setting a resource, use
    :class:`ResourceCacheKey` for the key, not ``*args`` and ``**kwargs``.

    Editing the max size of the cache after initialization is supported, and
    will evict least frequently used items until the cache size is within the
    new limit if the new maximum size is less than the current number of items
    in the cache.

    Attempting to overwrite an existing resource in the cache will raise an
    error.

    ``LFUResourceCache`` does not support ``fromkeys``, ``update``,
    ``setdefault``, the ``|=`` operator, or the ``|`` operator.

    .. versionadded:: 2.0.0

    Parameters
    ----------
    max_size : int, optional
        The maximum number of resources to store in the cache. Defaults to 10.

    Attributes
    ----------
    max_size : int, optional
        The maximum number of resources to store in the cache. Defaults to 10.

    Methods
    -------
    clear() -> None
        Clears all resources from the cache.
    copy() -> LFUResourceCache
        Returns a shallow copy of the cache.
    get(
        key: ResourceCacheKey, default: ServiceResource = None
    ) -> ServiceResource | None
        Gets the resource associated with the given key, or returns the
        default.
    items() -> Tuple[Tuple[ResourceCacheKey, ServiceResource], ...]
        Returns the items in the cache as (key, resource) tuples.
    keys() -> Tuple[ResourceCacheKey, ...]
        Returns the keys in the cache.
    pop(key: ResourceCacheKey) -> ServiceResource
        Pops and returns the resource associated with the given key.
    popitem() -> Tuple[ResourceCacheKey, ServiceResource]
        Pops and returns the least frequently used resource as a (key,
        resource) tuple.
    values() -> Tuple[ServiceResource, ...]
        Returns the resources in the cache.

    Raises
    ------
    ResourceCacheError
        Raised when an error occurs related to cache operations, such as using
        an invalid key or value type.
    ResourceCacheExistsError
        Raised when attempting to add a resource which already exists in the
        cache.
    ResourceCacheNotFoundError
        Raised when attempting to retrieve or delete a resource which does not
        exist in the cache.

    References
    ----------
    .. [1] "An O(1) algorithm for implementing the LFU cache eviction scheme",
       http://dhruvbird.com/lfu.pdf

    Examples
    --------
    >>> from boto3_client_cache import LFUResourceCache, ResourceCacheKey
    >>> cache = LFUResourceCache(max_size=2)
    >>> kwargs = {"service_name": "s3", "region_name": "us-west-2"}
    >>> cache[ResourceCacheKey(**kwargs)] = boto3.resource(**kwargs)
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(cache_type="resource", *args, **kwargs)


class ClientCache:
    """Core interface for creating client cache instances.

    .. versionadded:: 0.1.0

    Parameters
    ----------
    eviction_policy : EvictionPolicy, optional
        The type of cache to create. Case sensitive. Options are "LRU" and
        "LFU". Defaults to "LRU".
    *args : Any, optional
        Positional arguments to pass to the cache constructor. Refer to the
        docs for :class:`LRUClientCache` and :class:`LFUClientCache` for
        supported arguments.
    **kwargs : Any, optional
        Keyword arguments to pass to the cache constructor. Refer to the
        docs for :class:`LRUClientCache` and :class:`LFUClientCache` for
        supported arguments.

    Returns
    -------
    LRUClientCache | LFUClientCache
        An LRU or LFU client cache instance.

    Raises
    ------
    ClientCacheError
        Raised when an error occurs related to cache operations, such as using
        an invalid key, eviction policy, or value type.
    ClientCacheExistsError
        Raised when attempting to add a client which already exists in the
        cache.
    ClientCacheNotFoundError
        Raised when attempting to retrieve or delete a client which does not
        exist in the cache.

    See Also
    --------
    boto3_client_cache.cache.LFUClientCache
    boto3_client_cache.cache.LRUClientCache

    Examples
    --------
    LRU cache example:

    >>> from boto3_client_cache import ClientCache, ClientCacheKey
    >>> cache = ClientCache(max_size=2)
    >>> kwargs = {"service_name": "s3", "region_name": "us-west-2"}
    >>> cache[ClientCacheKey(**kwargs)] = boto3.client(**kwargs)

    LFU cache example:

    >>> from boto3_client_cache import ClientCache, ClientCacheKey
    >>> cache = ClientCache("LFU", max_size=2)
    >>> kwargs = {"service_name": "s3", "region_name": "us-west-2"}
    >>> cache[ClientCacheKey(**kwargs)] = boto3.client(**kwargs)
    """

    def __new__(
        cls, eviction_policy: EvictionPolicy | None = None, *args, **kwargs
    ) -> LRUClientCache | LFUClientCache:
        # default to LRU if eviction_policy is None or empty string
        eviction_policy = eviction_policy or "LRU"

        if (
            eviction_policy not in _ClientCacheRegistry.registry
            or not isinstance(eviction_policy, str)
        ):
            raise ClientCacheError(
                f"Unsupported cache type: '{eviction_policy}'. "
                "Supported cache types are: "
                f"{','.join(_ClientCacheRegistry.registry.keys())}"
            ) from None

        return _ClientCacheRegistry.registry[eviction_policy](*args, **kwargs)


class ResourceCache:
    """Core interface for creating resource cache instances.

    .. versionadded:: 2.0.0

    Parameters
    ----------
    eviction_policy : EvictionPolicy, optional
        The type of cache to create. Case sensitive. Options are "LRU" and
        "LFU". Defaults to "LRU".
    *args : Any, optional
        Positional arguments to pass to the cache constructor. Refer to the
        docs for :class:`LRUResourceCache` and :class:`LFUResourceCache` for
        supported arguments.
    **kwargs : Any, optional
        Keyword arguments to pass to the cache constructor. Refer to the
        docs for :class:`LRUResourceCache` and :class:`LFUResourceCache` for
        supported arguments.

    Returns
    -------
    LRUResourceCache | LFUResourceCache
        An LRU or LFU resource cache instance.

    Raises
    ------
    ResourceCacheError
        Raised when an error occurs related to cache operations, such as using
        an invalid key, eviction policy, or value type.
    ResourceCacheExistsError
        Raised when attempting to add a resource which already exists in the
        cache.
    ResourceCacheNotFoundError
        Raised when attempting to retrieve or delete a resource which does not
        exist in the cache.

    See Also
    --------
    boto3_client_cache.cache.LFUResourceCache
    boto3_client_cache.cache.LRUResourceCache

    Examples
    --------
    LRU cache example:

    >>> from boto3_client_cache import ResourceCache, ResourceCacheKey
    >>> cache = ResourceCache(max_size=2)
    >>> kwargs = {"service_name": "s3", "region_name": "us-west-2"}
    >>> cache[ResourceCacheKey(**kwargs)] = boto3.resource(**kwargs)

    LFU cache example:

    >>> from boto3_client_cache import ResourceCache, ResourceCacheKey
    >>> cache = ResourceCache("LFU", max_size=2)
    >>> kwargs = {"service_name": "s3", "region_name": "us-west-2"}
    >>> cache[ResourceCacheKey(**kwargs)] = boto3.resource(**kwargs)
    """

    def __new__(
        cls, eviction_policy: EvictionPolicy | None = None, *args, **kwargs
    ) -> LRUResourceCache | LFUResourceCache:
        # default to LRU if eviction_policy is None or empty string
        eviction_policy = eviction_policy or "LRU"

        if (
            eviction_policy not in _ResourceCacheRegistry.registry
            or not isinstance(eviction_policy, str)
        ):
            raise ResourceCacheError(
                f"Unsupported cache type: '{eviction_policy}'. "
                "Supported cache types are: "
                f"{','.join(_ResourceCacheRegistry.registry.keys())}"
            ) from None

        return _ResourceCacheRegistry.registry[eviction_policy](
            *args, **kwargs
        )
