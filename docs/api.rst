.. _api:

API
===

The primary interfaces for boto3-client-cache are :class:`boto3_client_cache.cache.ClientCache` and :class:`boto3_client_cache.cache.ResourceCache`,
which can be used to cache and retrieve boto3 clients and resources efficiently. The cache is designed to be thread-safe and supports LRU and LFU eviction policies.

:class:`boto3_client_cache.cache.ClientCache` and :class:`boto3_client_cache.cache.ResourceCache` can be used exactly like a standard Python 
dictionary, with the exception of the ``fromkeys``, ``update``, and ``setdefault`` methods, as 
well as the ``|=`` and ``|`` operator.

.. important::

   To interact with the cache, you **must** use the :class:`boto3_client_cache.cache.ClientCacheKey` or :class:`boto3_client_cache.cache.ResourceCacheKey`
   objects to create unique keys and fetch clients or resources. Additionally, assignments **must** be boto3 client or resource objects.

Modules
-------

Refer to the following modules for more details on configuration, implementation, and 
available exceptions.

.. autosummary::
   :toctree: reference
   :recursive:

   boto3_client_cache.cache
   boto3_client_cache.exceptions
   boto3_client_cache.session
