# boto3-client-cache

<div align="left">

  <a href="https://pypi.org/project/boto3-client-cache/">
    <img 
      src="https://img.shields.io/pypi/v/boto3-client-cache?color=%23f86d8e&logo=python&label=Latest%20Version"
      alt="pypi_version"
    />
  </a>

  <a href="https://pypi.org/project/boto3-client-cache/">
    <img 
      src="https://img.shields.io/pypi/pyversions/boto3-client-cache?style=pypi&color=%23f86d8e&logo=python&label=Compatible%20Python%20Versions" 
      alt="py_version"
    />
  </a>

  <a href="https://github.com/61418/boto3-client-cache/actions/workflows/push.yml">
    <img 
      src="https://img.shields.io/github/actions/workflow/status/61418/boto3-client-cache/push.yml?logo=github&color=%23f86d8e&label=Build" 
      alt="workflow"
    />
  </a>

  <a href="https://github.com/61418/boto3-client-cache/commits/main">
    <img 
      src="https://img.shields.io/github/last-commit/61418/boto3-client-cache?logo=github&color=%23f86d8e&label=Last%20Commit" 
      alt="last_commit"
    />
  </a>

  <a href="https://61418.io/boto3-client-cache">
    <img 
      src="https://img.shields.io/badge/Official%20Documentation-📘-f86d8e?style=flat&labelColor=555&logo=readthedocs" 
      alt="documentation"
    />
  </a>

  <a href="https://github.com/61418/boto3-client-cache">
    <img 
      src="https://img.shields.io/badge/Source%20Code-💻-f86d8e?style=flat&labelColor=555&logo=github" 
      alt="github"
    />
  </a>

  <a href="https://github.com/61418/boto3-client-cache/blob/main/LICENSE">
    <img 
      src="https://img.shields.io/static/v1?label=License&message=Apache&color=f86d8e&labelColor=555&logo=github&style=flat"
      alt="license"
    />
  </a>

</div>

</br>

## Description

boto3-client-cache provides a concurrency-safe, bounded cache for boto3 client and resource objects with deterministic identity semantics. LRU and LFU eviction are supported.

boto3-client-cache was authored by [Mike Letts](https://github.com/michaelthomasletts) and is maintained by [61418](https://github.com/61418).

## Why this exists

[boto3 clients and resources consume a large amount of memory](https://github.com/boto/boto3/issues/4568). Many developers never notice this. *At scale*, however, the memory footprint of boto3 clients and resources often becomes clear through manifold consequences. Caching is an obvious choice for managing multiple clients and-or resources at scale. 

boto3 does not cache client or resource objects natively. There are also, to my knowledge, no other open-source tools available which do what boto3-client-cache does. To compensate, bespoke caching solutions [circulate online](https://github.com/boto/boto3/issues/1670). boto3-client-cache exists to standardize and democratize client and resource caching for the Python AWS community.

## Design

The most important but challenging design choice for client and resource caching is selecting and enforcing a robust and standardized methodology for unique keys. **boto3-client-cache hashes according to boto3 client and resource signatures**. 

Setting and retrieving clients and resources from the client cache therefore requires an explicit declaration of intention -- that is, *the developer must explicitly pass client and resource initialization parameters to a `ClientCacheKey` or `ResourceCacheKey` object in order to set or retrieve boto3 clients*. This ensures setting and retrieving clients and resources are *unambiguous and deterministic* operations. By locking the cache, as boto3-client-cache does, race conditions are prevented, enabling developers to confidently employ the cache at scale with predictable cache eviction behavior. Lastly, by designing the cache like a dict in the standard Python library, the cache is ergonomically familiar and thus easy to use.

These decisions reflect the core design goals of boto3-client-cache: **safety at scale, deterministic behavior, ergonomic interfacing, and explicit identity**.

## Installation

```bash
pip install boto3-client-cache
```

## High-level API

The high-level API is ergonomically identical to boto3's API, but with caching capabilities built in.

To use it, you can initialize a client or resource which is automatically cached.

```python
from boto3_client_cache import client

# you can specify eviction_policy and max_size by passing them as args
# here, they are excluded so the defaults (eviction_policy="LRU", max_size=10) are used
s3 = client("s3", region_name="us-west-2")
s3_again = client("s3", region_name="us-west-2")
assert s3 is s3_again  # True
```

Or initialize a client or resource from a session directly.

```python
from boto3_client_cache import Session

session = Session(profile_name="default")
s3 = session.client("s3")
s3_again = session.client("s3")
assert s3 is s3_again  # True
```

## Low-level API

The low-level API offers more control and flexibility, allowing you to manage multiple caches with different eviction policies and configurations. The low-level API must be used in tandem with `boto3`.

```python
from boto3_client_cache import ClientCache, ClientCacheKey
import boto3

# create an LRU client cache with a maximum size of 30
cache = ClientCache(max_size=30)

# store boto3 client params in an object
kwargs = {"service_name": "s3", "region_name": "us-west-2"}

# create a cache key using those params
key = ClientCacheKey(**kwargs)

# assign a client
cache[key] = boto3.client(**kwargs)

# and retrieve that client using the key
s3_client = cache[key]
```

## Error semantics

Errors for resources are identical to errors for clients, except that the word "Client" is replaced with "Resource" in the exception name.

```python
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
```

## License

Beginning v2.1.3, boto3-client-cache is licensed under the [Mozilla Public License 2.0 (MPL-2.0)](https://github.com/61418/boto3-client-cache/blob/main/LICENSE). Earlier versions remain licensed under the Apache Software License 2.0.

## Contributing

Refer to the [contributing guidelines](https://github.com/61418/boto3-client-cache?tab=contributing-ov-file) for additional information on how to contribute to boto3-client-cache.

## Special thanks

- [Patrick Sanders](https://github.com/patricksanders)
- [Ben Kehoe](https://github.com/benkehoe)
