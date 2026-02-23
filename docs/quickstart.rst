.. _quickstart:

Quickstart
==========

The following examples demonstrate how to use the high-level and low-level APIs for client and resource caching with both LRU and LFU eviction policies, as well as how to handle and anticipate common exceptions.

High-level API
--------------

The high-level API is ergonomically identical to boto3's API, but with caching capabilities built in.

To use it, you can initialize a client or resource which is automatically cached.

.. code-block:: python

    from boto3_client_cache import client

    # you can specify eviction_policy and max_size by passing them as args
    # here, they are excluded so the defaults (eviction_policy="LRU", max_size=10) are used
    s3 = client("s3", region_name="us-west-2")
    s3_again = client("s3", region_name="us-west-2")
    assert s3 is s3_again

Or initialize a client or resource from a session directly.

.. code-block:: python

    from boto3_client_cache import Session

    session = Session(profile_name="default")
    s3 = session.client("s3")
    s3_again = session.client("s3")
    assert s3 is s3_again

Low-level API
-------------

The low-level API offers more control and flexibility, allowing you to manage multiple caches with different eviction policies and configurations. 
The low-level API must be used in tandem with `boto3`.

LRU cache for boto3 clients
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Initialize an LRU client cache, set a client in the cache, and retrieve it using the same key.

.. code-block:: python

    from boto3_client_cache import ClientCache, ClientCacheKey
    import boto3

    # LRU is the default
    cache = ClientCache(max_size=30)
    kwargs = {"service_name": "s3", "region_name": "us-west-2"}
    key = ClientCacheKey(**kwargs)
    cache[key] = boto3.client(**kwargs)
    s3_client = cache[key]

LRU cache for boto3 resources
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Initialize an LRU resource cache, set a resource in the cache, and retrieve it using the same key.

.. code-block:: python

    from boto3_client_cache import ResourceCache, ResourceCacheKey
    import boto3

    # LRU is the default
    cache = ResourceCache(max_size=30)
    kwargs = {"service_name": "s3", "region_name": "us-west-2"}
    key = ResourceCacheKey(**kwargs)
    cache[key] = boto3.resource(**kwargs)
    s3_resource = cache[key]

LFU cache for boto3 clients
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Initialize an LFU client cache, set a client in the cache, and retrieve it using the same key.

.. code-block:: python

    from boto3_client_cache import ClientCache, ClientCacheKey
    import boto3

    # LRU is the default
    cache = ClientCache("LFU", 30)
    kwargs = {"service_name": "s3", "region_name": "us-west-2"}
    key = ClientCacheKey(**kwargs)
    cache[key] = boto3.client(**kwargs)
    s3_client = cache[key]

LFU cache for boto3 resources
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Initialize an LFU resource cache, set a resource in the cache, and retrieve it using the same key.

.. code-block:: python

    from boto3_client_cache import ResourceCache, ResourceCacheKey
    import boto3

    # LRU is the default
    cache = ResourceCache("LFU", 30)
    kwargs = {"service_name": "s3", "region_name": "us-west-2"}
    key = ResourceCacheKey(**kwargs)
    cache[key] = boto3.resource(**kwargs)
    s3_resource = cache[key]

Error Semantics
---------------

Errors for resources are identical to errors for clients, except that the word "Client" is replaced with "Resource" in the exception name.

.. code-block:: python

    # raises ClientCacheExistsError b/c client(**kwargs) already exists
    cache[key] = boto3.client(**kwargs)

    # raises ClientCacheNotFoundError b/c the specific client was not cached
    cache[ClientCacheKey(service_name="ec2", region_name="us-west-2")]

    # returns None instead of raising ClientCacheNotFoundError
    cache.get(ClientCacheKey(service_name="ec2", region_name="us-west-2"))

    # raises ClientCacheError b/c the key is not a ClientCacheKey
    cache["this is not a ClientCacheKey"]

    # raises ClientCacheError b/c the object is not a client
    cache[ClientCacheKey("s3")] = "this is not a boto3 client"