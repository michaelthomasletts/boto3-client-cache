boto3-client-cache
==================

**Version:** |release|

**License:** `Apache License 2.0 <https://github.com/michaelthomasletts/boto3-client-cache/blob/main/LICENSE>`_

**Author:** `Mike Letts <https://michaelthomasletts.com>`_

Description
-----------

boto3-client-cache provides a concurrency-safe, bounded cache for boto3 clients with deterministic identity semantics.

LRU eviction is supported. LFU eviction is planned for a future release.

Why this Exists
---------------

`boto3 clients consume a large amount of memory <https://github.com/boto/boto3/issues/4568>`_. 
Many developers never notice this. 
*At scale*, however, the memory footprint of boto3 clients often becomes clear through manifold consequences. 
Client caching is an obvious choice for managing multiple clients at scale.

Design
------

The most important but challenging design choice for client caching is selecting and enforcing a robust and standardized methodology for unique keys. 
**boto3-client-cache hashes according to boto3 client signatures**. 

Setting and retrieving clients from the client cache therefore requires an explicit declaration of intention -- that is, *the developer must explicitly pass client initialization parameters to a ClientCacheKey object in order to set or retrieve boto3 clients*. 
This ensures setting and retrieving clients are *unambiguous and deterministic* operations. 
By locking the client cache, as boto3-client-cache does, race conditions are prevented, enabling developers to confidently employ the client cache at scale with predictable cache eviction behavior. 
Lastly, by designing the cache like a dict in the standard Python library, the cache is ergonomically familiar and thus easy to use.

These decisions reflect the core design goals of boto3-client-cache: **safety at scale, deterministic behavior, ergonomic interfacing, and explicit identity**.

.. toctree::
   :maxdepth: 1
   :name: sitemap
   :hidden:

   Installation <installation>
   Quickstart <quickstart>
   API <api>