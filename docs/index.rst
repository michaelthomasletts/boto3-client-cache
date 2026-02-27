boto3-client-cache
==================

**Version:** |release|

**License:** `Mozilla Public License 2.0 <https://github.com/61418/boto3-client-cache/blob/main/LICENSE>`_

**Author:** `Mike Letts <https://michaelthomasletts.com>`_

Description
-----------

boto3-client-cache provides a concurrency-safe, bounded cache for boto3 client and resource objects with deterministic identity semantics. LRU and LFU eviction are supported.

Why this Exists
---------------

`boto3 clients and resources consume a large amount of memory <https://github.com/boto/boto3/issues/4568>`_. 
Many developers never notice this. 
*At scale*, however, the memory footprint of boto3 clients and resources often becomes clear through manifold consequences. 
Caching is an obvious choice for managing multiple clients and-or resources at scale.

boto3 does not cache client or resource objects natively. 
There are also, to my knowledge, no other open-source tools available which do what boto3-client-cache does.
To compensate, bespoke caching solutions `circulate online <https://github.com/boto/boto3/issues/1670>`_.
boto3-client-cache exists to standardize and democratize client and resource caching for the Python AWS community.

Design
------

The most important but challenging design choice for client and resource caching is selecting and enforcing a robust and standardized methodology for unique keys. 
**boto3-client-cache hashes according to boto3 client and resource signatures**. 

Setting and retrieving clients and resources from the cache therefore requires an explicit declaration of intention -- that is, *the developer must explicitly pass client or resource initialization parameters to a ClientCacheKey or ResourceCacheKey object in order to set or retrieve boto3 clients or resources*. 
This ensures setting and retrieving clients and resources are *unambiguous and deterministic* operations. 
By locking the cache, as boto3-client-cache does, race conditions are prevented, enabling developers to confidently employ the cache at scale with predictable cache eviction behavior. 
Lastly, by designing the cache like a dict in the standard Python library, the cache is ergonomically familiar and thus easy to use.

These decisions reflect the core design goals of boto3-client-cache: **safety at scale, deterministic behavior, ergonomic interfacing, and explicit identity**.

.. toctree::
   :maxdepth: 1
   :name: sitemap
   :hidden:

   Installation <installation>
   Quickstart <quickstart>
   API <api>