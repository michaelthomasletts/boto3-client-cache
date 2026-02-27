# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""High-level API which provides a subclass of :class:`boto3.session.Session`
that implements automatic caching for clients and resources. It also provides
functions to set up the default session with caching capabilities.

Examples
--------

Probably the easiest way to use this library is by using ``client``:

>>> from boto3_client_cache import client
>>> s3_client = client("s3")
>>> s3_client_again = client("s3")
>>> assert s3_client is s3_client_again  # True, since the client is cached

You can create a session by instantiating the ``Session`` class:

>>> from boto3_client_cache import Session
>>> session = Session(region_name="us-east-1")
>>> s3_client = session.client("s3")
>>> s3_client_again = session.client("s3")
>>> assert s3_client is s3_client_again  # True, since the client is cached

You can also set a default session:

>>> from boto3_client_cache import client, setup_default_session
>>> setup_default_session(region_name="us-east-1")
>>> s3_client = client("s3")
>>> s3_client_again = client("s3")
>>> assert s3_client is s3_client_again  # True, since the client is cached
"""

__all__ = [
    "Session",
    "SessionCache",
    "SessionClientCache",
    "SessionResourceCache",
    "client",
    "resource",
    "setup_default_session",
]

from typing import cast, get_args

import boto3
from boto3.resources.base import ServiceResource
from botocore.client import BaseClient

from .cache import (
    CacheType,
    ClientCache,
    ClientCacheKey,
    EvictionPolicy,
    ResourceCache,
    ResourceCacheKey,
)
from .exceptions import ClientCacheError, ResourceCacheError


class SessionClientCache:
    """Class representing the client cache for a session, which contains
    separate caches for different eviction policies (LRU and LFU).

    .. versionadded:: 2.1.0
    """

    def __init__(self) -> None:
        self.LRU = ClientCache()
        self.LFU = ClientCache("LFU")

    def __getitem__(self, key: EvictionPolicy):
        return getattr(self, key)


class SessionResourceCache:
    """Class representing the resource cache for a session, which contains
    separate caches for different eviction policies (LRU and LFU).

    .. versionadded:: 2.1.0
    """

    def __init__(self) -> None:
        self.LRU = ResourceCache()
        self.LFU = ResourceCache("LFU")

    def __getitem__(self, key: EvictionPolicy):
        return getattr(self, key)


class SessionCache:
    """Class representing the cache for a session, which contains separate
    client and resource caches for different eviction policies.

    .. versionadded:: 2.1.0
    """

    def __init__(self) -> None:
        self.client = SessionClientCache()
        self.resource = SessionResourceCache()

    def __getitem__(self, key: CacheType):
        return getattr(self, key)


class Session(boto3.Session):
    """A subclass of :class:`boto3.session.Session` which implements automatic
    caching for clients and resources.

    .. versionadded:: 2.1.0

    Parameters
    ----------
    *args
        Positional arguments to be passed to the parent class. Refer to the
        :class:`boto3.session.Session` documentation for more details on accepted
        arguments.
    **kwargs
        Keyword arguments to be passed to the parent class. Refer to the
        :class:`boto3.session.Session` documentation for more details on accepted
        arguments.

    Attributes
    ----------
    cache : CacheTypedDict
        A dictionary containing the client and resource caches for different
        eviction policies.

    Methods
    -------
    client(*args, eviction_policy: EvictionPolicy, max_size: int, **kwargs) -> BaseClient
        Returns a cached client if it exists, otherwise creates a new client
        and caches it.
    resource(*args, eviction_policy: EvictionPolicy, max_size: int, **kwargs) -> ServiceResource
        Returns a cached resource if it exists, otherwise creates a new
        resource and caches it.

    Notes
    -----
    .. important::

        The cache is not globally shared across all sessions. Each session
        maintains its own cache, so modifications to the cache in one session
        will not affect other sessions. To manage a cache across multiple
        sessions, use the low-level API instead (i.e.
        :class:`boto3_client_cache.ResourceCache` and
        :class:`boto3_client_cache.ClientCache`) in tandem with ``boto3``.

    Examples
    --------
    >>> from boto3_client_cache import Session
    >>> session = Session(region_name="us-east-1")
    >>> s3_client = session.client("s3")
    >>> s3_client_again = session.client("s3")
    >>> s3_client is s3_client_again
    True
    """  # noqa: E501

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # initializing client cache attribute
        self.cache: SessionCache = SessionCache()

    def client(
        self,
        *args,
        eviction_policy: EvictionPolicy | None = None,
        max_size: int | None = None,
        **kwargs,
    ) -> BaseClient:
        """Returns a cached client from the default session if it exists,
        otherwise creates a new client and caches it.

        .. versionadded:: 2.1.0

        Parameters
        ----------
        eviction_policy : EvictionPolicy, optional
            The type of cache to create. Case sensitive. Options are "LRU" and
            "LFU". Defaults to "LRU".
        max_size : int | None, optional
            The maximum size of the client cache. If None, the cache size is
            unlimited. Beware that modifying this value after the cache has
            already been initialized may evict existing clients. Default is
            None.
        *args
            Positional arguments to be passed to the default session's client
            method. Check :meth:`boto3.session.Session.client` for more details
            on accepted arguments.
        **kwargs
            Keyword arguments to be passed to the default session's client
            method. Check :meth:`boto3.session.Session.client` for more details
            on accepted arguments.

        Returns
        -------
        BaseClient
            A cached client if it exists, otherwise a new client that has been
            cached.

        Raises
        ------
        ClientCacheError
            Raised when an error occurs related to cache operations, such as
            using an invalid key, eviction policy, or value type.

        ClientCacheExistsError
            Raised when attempting to add a client which already exists in
            the cache.

        ClientCacheNotFoundError
            Raised when attempting to retrieve or delete a client which does
            not exist in the cache.

        Examples
        --------
        >>> from boto3_client_cache import client
        >>> s3_client = client("s3", region_name="us-east-1")
        >>> s3_client_again = client("s3", region_name="us-east-1")
        >>> s3_client is s3_client_again
        True
        """

        try:
            # validating eviction policy
            assert eviction_policy is None or eviction_policy in get_args(
                EvictionPolicy
            )
        except AssertionError:
            raise ClientCacheError(
                f"Invalid eviction policy: {eviction_policy}. "
                f"Valid options are: {get_args(EvictionPolicy)}."
            )

        # setting default eviction policy to "LRU" if None is provided
        eviction_policy = eviction_policy or "LRU"

        # creating a cache key based on the client initialization parameters
        key = ClientCacheKey(*args, **kwargs)

        # initializing the client if it doesn't exist in the cache yet
        if key not in self.cache.client[eviction_policy]:
            self.cache["client"][eviction_policy][key] = super().client(
                *args, **kwargs
            )

        # updating the max_size of the client cache if it has changed
        if (
            max_size is not None
            and max_size != self.cache["client"][eviction_policy].max_size
        ):
            self.cache["client"][eviction_policy].max_size = max_size

        return self.cache["client"][eviction_policy][key]

    def resource(  # type: ignore[override]
        self,
        *args,
        eviction_policy: EvictionPolicy | None = None,
        max_size: int | None = None,
        **kwargs,
    ) -> ServiceResource:
        """Returns a cached resource from the default session if it exists,
        otherwise creates a new resource and caches it.

        .. versionadded:: 2.1.0

        Parameters
        ----------
        eviction_policy : EvictionPolicy, optional
            The type of cache to create. Case sensitive. Options are "LRU" and
            "LFU". Defaults to "LRU".
        max_size : int | None, optional
            The maximum size of the resource cache. If None, the cache size is
            unlimited. Beware that modifying this value after the cache has
            already been initialized may evict existing resources. Default is
            None.
        *args
            Positional arguments to be passed to the default session's resource
            method. Check :meth:`boto3.session.Session.resource` for more
            details on accepted arguments.
        **kwargs
            Keyword arguments to be passed to the default session's resource
            method. Check :meth:`boto3.session.Session.resource` for more
            details on accepted arguments.

        Returns
        -------
        ServiceResource
            A cached resource if it exists, otherwise a new resource that has
            been cached.

        Raises
        ------
        ResourceCacheError
            Raised when an error occurs related to cache operations, such as
            using an invalid key, eviction policy, or value type.

        ResourceCacheExistsError
            Raised when attempting to add a resource which already exists in
            the cache.

        ResourceCacheNotFoundError
            Raised when attempting to retrieve or delete a resource which does
            not exist in the cache.

        Notes
        -----
        .. tip::

            For correct typing, you may want to import mypy-boto3-* and use the
            generated type annotations for casting clients, which will be
            compatible with this method.

        Examples
        --------
        >>> from boto3_client_cache import resource
        >>> s3_resource = resource("s3", region_name="us-east-1")
        >>> s3_resource_again = resource("s3", region_name="us-east-1")
        >>> s3_resource is s3_resource_again
        True
        """

        try:
            # validating eviction policy
            assert eviction_policy is None or eviction_policy in get_args(
                EvictionPolicy
            )
        except AssertionError:
            raise ResourceCacheError(
                f"Invalid eviction policy: {eviction_policy}. "
                f"Valid options are: {get_args(EvictionPolicy)}."
            )

        # setting default eviction policy to "LRU" if None is provided
        eviction_policy = eviction_policy or "LRU"

        # creating a cache key based on the resource initialization parameters
        key = ResourceCacheKey(*args, **kwargs)

        # initializing the resource if it doesn't exist in the cache yet
        if key not in self.cache["resource"][eviction_policy]:
            self.cache["resource"][eviction_policy][key] = super().resource(
                *args, **kwargs
            )

        # updating the max_size of the resource cache if it has changed
        if (
            max_size is not None
            and max_size != self.cache["resource"][eviction_policy].max_size
        ):
            self.cache["resource"][eviction_policy].max_size = max_size

        return self.cache["resource"][eviction_policy][key]


def setup_default_session(**kwargs) -> Session:
    """Sets up the default session with caching capabilities.

    .. versionadded:: 2.1.0

    Parameters
    ----------
    **kwargs
        Keyword arguments to be passed to the default session.

    Returns
    -------
    Session
        The default session with caching capabilities.
    """

    boto3.DEFAULT_SESSION = Session(**kwargs)
    return cast(Session, boto3.DEFAULT_SESSION)


def _get_default_session() -> Session:
    if boto3.DEFAULT_SESSION is None:
        setup_default_session()

    return cast(Session, boto3.DEFAULT_SESSION)


def client(
    *args,
    eviction_policy: EvictionPolicy | None = None,
    max_size: int | None = None,
    **kwargs,
) -> BaseClient:
    """Returns a cached client from the default session if it exists,
    otherwise creates a new client and caches it.

    .. versionadded:: 2.1.0

    Parameters
    ----------
    eviction_policy : EvictionPolicy, optional
        The type of cache to create. Case sensitive. Options are "LRU" and
        "LFU". Defaults to "LRU".
    max_size : int | None, optional
        The maximum size of the client cache. If None, the cache size is
        unlimited. Beware that modifying this value after the cache has
        already been initialized may evict existing clients. Default is
        None.
    *args
        Positional arguments to be passed to the default session's client
        method. Check :meth:`boto3.session.Session.client` for more details
        on accepted arguments.
    **kwargs
        Keyword arguments to be passed to the default session's client method.
        Check :meth:`boto3.session.Session.client` for more details on accepted
        arguments.

    Returns
    -------
    BaseClient
        A cached client if it exists, otherwise a new client that has been
        cached.

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

    Examples
    --------
    >>> from boto3_client_cache import client
    >>> s3_client = client("s3", region_name="us-east-1")
    >>> s3_client_again = client("s3", region_name="us-east-1")
    >>> s3_client is s3_client_again
    True
    """

    return _get_default_session().client(
        *args, eviction_policy=eviction_policy, max_size=max_size, **kwargs
    )


def resource(
    *args,
    eviction_policy: EvictionPolicy | None = None,
    max_size: int | None = None,
    **kwargs,
) -> ServiceResource:
    """Returns a cached resource from the default session if it exists,
    otherwise creates a new resource and caches it.

    .. versionadded:: 2.1.0

    Parameters
    ----------
    eviction_policy : EvictionPolicy, optional
        The type of cache to create. Case sensitive. Options are "LRU" and
        "LFU". Defaults to "LRU".
    max_size : int | None, optional
        The maximum size of the resource cache. If None, the cache size is
        unlimited. Beware that modifying this value after the cache has
        already been initialized may evict existing resources. Default is None.
    *args
        Positional arguments to be passed to the default session's resource
        method. Check :meth:`boto3.session.Session.resource` for more details
        on accepted arguments.
    **kwargs
        Keyword arguments to be passed to the default session's resource
        method. Check :meth:`boto3.session.Session.resource` for more details
        on accepted arguments.

    Returns
    -------
    ServiceResource
        A cached resource if it exists, otherwise a new resource that has been
        cached.

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

    Notes
    -----
    .. tip::

        For correct typing, you may want to import mypy-boto3-* and use the
        generated type annotations for casting clients, which will be
        compatible with this method.

    Examples
    --------
    >>> from boto3_client_cache import resource
    >>> s3_resource = resource("s3", region_name="us-east-1")
    >>> s3_resource_again = resource("s3", region_name="us-east-1")
    >>> s3_resource is s3_resource_again
    True
    """

    return _get_default_session().resource(
        *args, eviction_policy=eviction_policy, max_size=max_size, **kwargs
    )
