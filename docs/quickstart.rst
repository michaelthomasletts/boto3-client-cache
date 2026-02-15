.. _quickstart:

Quickstart
==========

.. code-block:: python

    from boto3_client_cache import ClientCache, ClientCacheKey
    import boto3

    # create an LRU client cache with a maximum size of 30
    cache = ClientCache(max_size=30)

    # store boto3 client params in an object
    kwargs = {"service_name": "s3", "region_name": "us-west-2"}

    # create a cache key using those params
    key = ClientCacheKey(**kwargs)

    # make the assignment
    cache[key] = boto3.client(**kwargs)

    # and retrieve the client using the key
    s3_client = cache[key]

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