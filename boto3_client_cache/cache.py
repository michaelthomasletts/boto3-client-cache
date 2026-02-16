"""Module for caching boto3 clients based on their initialization
parameters."""

__all__ = [
    "CacheType",
    "ClientCache",
    "ClientCacheKey",
    "LFUClientCache",
    "LRUClientCache",
]

from abc import ABC, abstractmethod
from collections import OrderedDict
from collections.abc import Iterator
from threading import RLock
from typing import Any, Literal, Tuple

from botocore.client import BaseClient
from botocore.config import Config

from .exceptions import (
    ClientCacheError,
    ClientCacheExistsError,
    ClientCacheNotFoundError,
)

#: Type alias for supported cache types
CacheType = Literal["LRU", "LFU"]


class ClientCacheKey:
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
        # initializing the cache key and label
        self._create(*args, **kwargs)

    def __str__(self) -> str:
        return self.label

    def __repr__(self) -> str:
        return f"ClientCacheKey(client({self.label}))"

    def __hash__(self) -> int:
        return hash(self.key)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, ClientCacheKey) and self.key == other.key

    def _create(self, *args, **kwargs) -> None:
        """Creates the cache key and label based on the provided arguments.

        Parameters
        ----------
        *args : Any, optional
            Positional arguments used to create the cache key.
        **kwargs : Any, optional
            Keyword arguments used to create the cache key.
        """

        # creating a readable label for debugging purposes
        self.label: str = ", ".join(
            [
                *(repr(a) for a in args),
                *(
                    f"{k}={self._format_label_value(v)}"
                    for k, v in sorted(kwargs.items())
                ),
            ]
        )

        # checking if Config was passed as a positional arg
        _args = [
            self._config_cache_key(arg) if isinstance(arg, Config) else arg
            for arg in args
        ]

        # popping trailing None values from args, preserving None in middle
        while _args and _args[-1] is None:
            _args.pop()
        _args = tuple(_args)

        # checking if Config was passed as a keyword arg
        _kwargs = kwargs.copy()
        if _kwargs.get("config") is not None:
            _kwargs["config"] = self._config_cache_key(_kwargs["config"])

        # preemptively removing None values from kwargs
        _kwargs = {
            key: value for key, value in _kwargs.items() if value is not None
        }

        # creating a unique key for the client cache
        self.key = (_args, tuple(sorted(_kwargs.items())))

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


class _AbstractClientCache(ABC):
    """Abstract base class for client caches.

    .. versionadded:: 0.1.0
    """

    @abstractmethod
    def __str__(self) -> str: ...

    @abstractmethod
    def __repr__(self) -> str: ...

    @abstractmethod
    def __len__(self) -> int: ...

    @abstractmethod
    def __reversed__(self) -> Iterator[ClientCacheKey]: ...

    @abstractmethod
    def __contains__(self, key: ClientCacheKey) -> bool: ...

    @abstractmethod
    def __iter__(self) -> Iterator[ClientCacheKey]: ...

    @abstractmethod
    def __getitem__(self, key: ClientCacheKey) -> BaseClient: ...

    @abstractmethod
    def __setitem__(self, key: ClientCacheKey, obj: BaseClient) -> None: ...

    @abstractmethod
    def __delitem__(self, key: ClientCacheKey) -> None: ...

    @abstractmethod
    def keys(self) -> Tuple[ClientCacheKey, ...]: ...

    @abstractmethod
    def values(self) -> Tuple[BaseClient, ...]: ...

    @abstractmethod
    def items(self) -> Tuple[Tuple[ClientCacheKey, BaseClient], ...]: ...

    @abstractmethod
    def get(
        self, key: ClientCacheKey, default: BaseClient | None = None
    ) -> BaseClient | None: ...

    @abstractmethod
    def pop(self, key: ClientCacheKey) -> BaseClient: ...

    @abstractmethod
    def clear(self) -> None: ...

    @abstractmethod
    def popitem(self) -> Tuple[ClientCacheKey, BaseClient]: ...

    @abstractmethod
    def copy(self) -> "_AbstractClientCache": ...


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
        cls, *, eviction_policy: CacheType, **kwargs: Any
    ) -> None:
        # calling the parent method to ensure proper subclass initialization
        super().__init_subclass__(**kwargs)

        # registering the subclass using the provided cache type
        cls.registry[eviction_policy] = cls


class LRUClientCache(
    _AbstractClientCache, _ClientCacheRegistry, eviction_policy="LRU"
):
    """A thread-safe LRU cache for storing clients which can be used exactly
    like a dictionary.

    Clients stored in this cache must be hashable. The cache has a maximum size
    attribute, and retrieved and newly added clients are marked as recently
    used. When the cache exceeds its maximum size, the least recently used
    client is evicted. When setting a client, use :class:`ClientCacheKey` for
    the key, not ``*args`` and ``**kwargs`` unless calling this class via
    ``__call__``.

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

    def __init__(self, max_size: int | None = None) -> None:
        self._max_size = abs(max_size if max_size is not None else 10)
        self._cache: OrderedDict[ClientCacheKey, BaseClient] = OrderedDict()
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

    def __call__(self, obj: BaseClient, *args, **kwargs) -> None:
        """Adds the given object to the cache using the provided arguments to
        create the cache key.

        Parameters
        ----------
        obj : BaseClient
            The client object to cache.
        *args : Any, optional
            Positional arguments used to create the cache key.
        **kwargs : Any, optional
            Keyword arguments used to create the cache key.

        Examples
        --------
        Using the ClientCache to cache an S3 client:

        >>> cache = ClientCache(max_size=10)
        >>> s3_client = boto3.client("s3")
        >>> cache(s3_client, "s3", region_name="us-west-2")
        """

        self.__setitem__(ClientCacheKey(*args, **kwargs), obj)

    def __str__(self) -> str:
        with self._lock:
            if not self._cache:
                return "ClientCache(empty)"
            labels = "\n   ".join(
                f"- RefreshableSession.client({key.label})"
                for key in self._cache.keys()
            )
            return f"ClientCache:\n   {labels}"

    def __repr__(self) -> str:
        return self.__str__()

    def __len__(self) -> int:
        with self._lock:
            return len(self._cache)

    def __reversed__(self) -> Iterator[ClientCacheKey]:
        with self._lock:
            return iter(tuple(reversed(self._cache.keys())))

    def __contains__(self, key: ClientCacheKey) -> bool:
        with self._lock:
            return key in self._cache

    def __iter__(self) -> Iterator[ClientCacheKey]:
        with self._lock:
            return iter(tuple(self._cache.keys()))

    def __getitem__(self, key: ClientCacheKey) -> BaseClient:
        with self._lock:
            # move obj to end of cache to mark it as recently used
            if key in self._cache:
                self._cache.move_to_end(key)
                return self._cache[key]
            else:
                raise ClientCacheNotFoundError(
                    "The client you requested has not been cached."
                ) from None

    def __setitem__(self, key: ClientCacheKey, obj: BaseClient) -> None:
        if not isinstance(key, ClientCacheKey):
            raise ClientCacheError(
                "Cache key must be of type 'ClientCacheKey'."
            ) from None

        if not isinstance(obj, BaseClient):
            raise ClientCacheError(
                f"Cache value must be a boto3 client object, {type(obj)} "
                "provided."
            ) from None

        with self._lock:
            if key in self._cache:
                raise ClientCacheExistsError(
                    "Client already exists in cache."
                ) from None

            # setting the object
            self._cache[key] = obj
            # marking the object as recently used
            self._cache.move_to_end(key)

            # removing least recently used object if cache exceeds max size
            if len(self._cache) > self.max_size:
                self._cache.popitem(last=False)

    def __delitem__(self, key: ClientCacheKey) -> None:
        with self._lock:
            if key not in self._cache:
                raise ClientCacheNotFoundError(
                    "Client not found in cache."
                ) from None
            del self._cache[key]

    def keys(self) -> Tuple[ClientCacheKey, ...]:
        """Returns the keys in the cache."""

        with self._lock:
            return tuple(self._cache.keys())

    def values(self) -> Tuple[BaseClient, ...]:
        """Returns the values from the cache."""

        with self._lock:
            return tuple(self._cache.values())

    def items(self) -> Tuple[Tuple[ClientCacheKey, BaseClient], ...]:
        """Returns the items in the cache as (ClientCacheKey, BaseClient)
        tuples."""

        with self._lock:
            return tuple(self._cache.items())

    def get(
        self, key: ClientCacheKey, default: BaseClient | None = None
    ) -> BaseClient | None:
        """Gets the object using the given key, or returns the default."""

        with self._lock:
            # move obj to end of cache to mark it as recently used
            if key in self._cache:
                self._cache.move_to_end(key)
            return self._cache.get(key, default)

    def pop(self, key: ClientCacheKey) -> BaseClient:
        """Pops and returns the object associated with the given key."""

        with self._lock:
            if (obj := self._cache.get(key)) is None:
                raise ClientCacheNotFoundError(
                    "Client not found in cache."
                ) from None
            del self._cache[key]
            return obj

    def clear(self) -> None:
        """Clears all items from the cache."""

        with self._lock:
            self._cache.clear()

    def popitem(self) -> Tuple[ClientCacheKey, BaseClient]:
        """Pops and returns the least recently used item from the cache."""

        with self._lock:
            if not self._cache:
                raise ClientCacheNotFoundError(
                    "No clients found in cache."
                ) from None
            return self._cache.popitem(last=False)

    def copy(self) -> "LRUClientCache":
        """Returns a shallow copy of the cache."""

        with self._lock:
            new_cache = LRUClientCache(max_size=self.max_size)
            new_cache._cache = self._cache.copy()
            return new_cache


class _FrequencyNode:
    """Internal node for a specific frequency in the LFU index."""

    def __init__(self, frequency: int) -> None:
        self.frequency = frequency
        self.keys: OrderedDict[ClientCacheKey, None] = OrderedDict()
        self.prev: "_FrequencyNode | None" = None
        self.next: "_FrequencyNode | None" = None


class _FrequencyIndex:
    """Internal O(1) index for LFU key ordering.

    Frequencies are stored in an ordered linked list. Each frequency node
    stores keys in an ``OrderedDict`` to preserve LRU tie-breaking among keys
    with the same access frequency.
    """

    def __init__(self) -> None:
        self._head: _FrequencyNode | None = None
        self._tail: _FrequencyNode | None = None
        self._key_to_node: dict[ClientCacheKey, _FrequencyNode] = {}

    def insert(self, key: ClientCacheKey) -> None:
        if self._head is None or self._head.frequency != 1:
            node = _FrequencyNode(1)
            self._insert_before(self._head, node)
        else:
            node = self._head

        node.keys[key] = None
        self._key_to_node[key] = node

    def increment(self, key: ClientCacheKey) -> None:
        current = self._key_to_node[key]
        target_frequency = current.frequency + 1
        target = current.next

        if target is None or target.frequency != target_frequency:
            target = _FrequencyNode(target_frequency)
            self._insert_after(current, target)

        del current.keys[key]
        target.keys[key] = None
        self._key_to_node[key] = target
        self._prune(current)

    def delete(self, key: ClientCacheKey) -> None:
        node = self._key_to_node.pop(key)
        del node.keys[key]
        self._prune(node)

    def pop_least_frequent(self) -> ClientCacheKey:
        if self._head is None:
            raise KeyError("No keys are available in the LFU index.")

        node = self._head
        key, _ = node.keys.popitem(last=False)
        del self._key_to_node[key]
        self._prune(node)
        return key

    def iter_keys(self) -> Iterator[ClientCacheKey]:
        current = self._head
        while current is not None:
            yield from current.keys
            current = current.next

    def iter_keys_reversed(self) -> Iterator[ClientCacheKey]:
        current = self._tail
        while current is not None:
            yield from reversed(current.keys)
            current = current.prev

    def copy(self) -> "_FrequencyIndex":
        clone = _FrequencyIndex()
        current = self._head
        previous_clone_node: _FrequencyNode | None = None

        while current is not None:
            clone_node = _FrequencyNode(current.frequency)
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
        reference: _FrequencyNode | None,
        node: _FrequencyNode,
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
        reference: _FrequencyNode,
        node: _FrequencyNode,
    ) -> None:
        next_node = reference.next
        reference.next = node
        node.prev = reference
        node.next = next_node

        if next_node is None:
            self._tail = node
        else:
            next_node.prev = node

    def _prune(self, node: _FrequencyNode) -> None:
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


class LFUClientCache(
    _AbstractClientCache, _ClientCacheRegistry, eviction_policy="LFU"
):
    """A thread-safe LFU cache [1]_ for storing clients which can be used
    exactly like a dictionary.

    Clients stored in this cache must be hashable. The cache has a maximum
    size attribute, and retrieved clients are promoted to a higher frequency
    bucket. When the cache exceeds its maximum size, the least frequently used
    client is evicted. If multiple clients share the same frequency, the least
    recently used client within that frequency bucket is evicted first. When
    setting a client, use :class:`ClientCacheKey` for the key, not ``*args``
    and ``**kwargs`` unless calling this class via ``__call__``.

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

    def __init__(self, max_size: int | None = None) -> None:
        self._max_size = abs(max_size if max_size is not None else 10)
        self._cache: dict[ClientCacheKey, BaseClient] = {}
        self._frequencies = _FrequencyIndex()
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
                evicted_key = self._frequencies.pop_least_frequent()
                del self._cache[evicted_key]

    def __call__(self, obj: BaseClient, *args, **kwargs) -> None:
        """Adds the given object to the cache using the provided arguments to
        create the cache key."""

        self.__setitem__(ClientCacheKey(*args, **kwargs), obj)

    def __str__(self) -> str:
        with self._lock:
            keys = tuple(self._frequencies.iter_keys())
            if not keys:
                return "ClientCache(empty)"
            labels = "\n   ".join(
                f"- RefreshableSession.client({key.label})" for key in keys
            )
            return f"ClientCache:\n   {labels}"

    def __repr__(self) -> str:
        return self.__str__()

    def __len__(self) -> int:
        with self._lock:
            return len(self._cache)

    def __reversed__(self) -> Iterator[ClientCacheKey]:
        with self._lock:
            return iter(tuple(self._frequencies.iter_keys_reversed()))

    def __contains__(self, key: ClientCacheKey) -> bool:
        with self._lock:
            return key in self._cache

    def __iter__(self) -> Iterator[ClientCacheKey]:
        with self._lock:
            return iter(tuple(self._frequencies.iter_keys()))

    def __getitem__(self, key: ClientCacheKey) -> BaseClient:
        with self._lock:
            if key not in self._cache:
                raise ClientCacheNotFoundError(
                    "The client you requested has not been cached."
                ) from None
            self._frequencies.increment(key)
            return self._cache[key]

    def __setitem__(self, key: ClientCacheKey, obj: BaseClient) -> None:
        if not isinstance(key, ClientCacheKey):
            raise ClientCacheError(
                "Cache key must be of type 'ClientCacheKey'."
            ) from None

        if not isinstance(obj, BaseClient):
            raise ClientCacheError(
                f"Cache value must be a boto3 client object, {type(obj)} "
                "provided."
            ) from None

        with self._lock:
            if key in self._cache:
                raise ClientCacheExistsError(
                    "Client already exists in cache."
                ) from None

            if self.max_size == 0:
                return

            if len(self._cache) >= self.max_size:
                evicted_key = self._frequencies.pop_least_frequent()
                del self._cache[evicted_key]

            self._cache[key] = obj
            self._frequencies.insert(key)

    def __delitem__(self, key: ClientCacheKey) -> None:
        with self._lock:
            if key not in self._cache:
                raise ClientCacheNotFoundError(
                    "Client not found in cache."
                ) from None
            self._frequencies.delete(key)
            del self._cache[key]

    def keys(self) -> Tuple[ClientCacheKey, ...]:
        """Returns the keys in LFU order.

        Keys are ordered by increasing frequency. Within each frequency bucket,
        keys are ordered least-recently used to most-recently used.
        """

        with self._lock:
            return tuple(self._frequencies.iter_keys())

    def values(self) -> Tuple[BaseClient, ...]:
        """Returns the values in LFU order."""

        with self._lock:
            keys = tuple(self._frequencies.iter_keys())
            return tuple(self._cache[key] for key in keys)

    def items(self) -> Tuple[Tuple[ClientCacheKey, BaseClient], ...]:
        """Returns the items in LFU order."""

        with self._lock:
            keys = tuple(self._frequencies.iter_keys())
            return tuple((key, self._cache[key]) for key in keys)

    def get(
        self, key: ClientCacheKey, default: BaseClient | None = None
    ) -> BaseClient | None:
        """Gets the object using the given key, or returns the default."""

        with self._lock:
            if key not in self._cache:
                return default
            self._frequencies.increment(key)
            return self._cache[key]

    def pop(self, key: ClientCacheKey) -> BaseClient:
        """Pops and returns the object associated with the given key."""

        with self._lock:
            obj = self._cache.get(key)
            if obj is None:
                raise ClientCacheNotFoundError(
                    "Client not found in cache."
                ) from None
            self._frequencies.delete(key)
            del self._cache[key]
            return obj

    def clear(self) -> None:
        """Clears all items from the cache."""

        with self._lock:
            self._cache.clear()
            self._frequencies = _FrequencyIndex()

    def popitem(self) -> Tuple[ClientCacheKey, BaseClient]:
        """Pops and returns the LFU item from the cache."""

        with self._lock:
            if not self._cache:
                raise ClientCacheNotFoundError(
                    "No clients found in cache."
                ) from None
            key = self._frequencies.pop_least_frequent()
            return key, self._cache.pop(key)

    def copy(self) -> "LFUClientCache":
        """Returns a shallow copy of the cache."""

        with self._lock:
            new_cache = LFUClientCache(max_size=self.max_size)
            new_cache._cache = self._cache.copy()
            new_cache._frequencies = self._frequencies.copy()
            return new_cache


class ClientCache:
    """Core interface for creating client cache instances.

    .. versionadded:: 0.1.0

    Parameters
    ----------
    eviction_policy : CacheType, optional
        The type of cache to create. Case sensitive. Options are "LRU" and
        "LFU". Defaults to "LRU".
    *args : Any, optional
        Positional arguments to pass to the cache constructor.
    **kwargs : Any, optional
        Keyword arguments to pass to the cache constructor.

    Returns
    -------
    LRUClientCache
        An LRU client cache instance.
    LFUClientCache
        An LFU client cache instance.

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
        cls, eviction_policy: CacheType = "LRU", *args, **kwargs
    ) -> LRUClientCache | LFUClientCache:
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
