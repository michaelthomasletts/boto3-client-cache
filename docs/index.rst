boto3-client-cache
==================

**Version:** |release|

**License:** `Apache License 2.0 <https://github.com/michaelthomasletts/boto3-client-cache/blob/main/LICENSE>`_

**Author:** `Mike Letts <https://michaelthomasletts.com>`_

Description
-----------

boto3-client-cache is a simple Python package which caches boto3 clients. 

Raison d'Être
-------------

**boto3 clients consume a large amount of memory**. 
Many developers never notice. 
*At scale*, however, this becomes painfully obvious. 
There is a clear incentive, therefore, to avoid initializing duplicate client objects. 
Client caching is an obvious solution.

The most challenging aspect of boto3 client caching is selecting robust and standardized unique keys. 
Managing ad-hoc keys at scale is unwieldy and insecure. **boto3-client-cache hashes according to client signatures**. 
Setting and retrieving clients from the client cache therefore requires an explicit declaration of intention -- that is, *the developer must explicitly pass client initialization parameters to a `CacheKey` object in order to set or retrieve a client*.

From a developer experience perspective, this design - that is, forcing `CacheKey` - may feel clunky; however, it ensures setting and retrieving clients are unambiguous operations. 
Further, locking the client cache, as boto3-client-cache does, prevents race conditions, enabling developers to confidently employ the client cache at scale.

Although boto3-client-cache can help any developer working with the AWS Python SDK at any scale, it was designed primarily for security, cloud, machine learning, and platform teams operating at scale. 

boto3-client-cache, it should be noted, is also a critical dependency for `boto3-client-cache <https://github.com/michaelthomasletts/boto3-client-cache>`_.

.. toctree::
   :maxdepth: 1
   :name: sitemap
   :hidden:

   Installation <installation>
   Quickstart <quickstart>
   API <api>