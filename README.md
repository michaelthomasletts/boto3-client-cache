# boto3-client-cache

<div align="left">

  <a href="https://pypi.org/project/boto3-client-cache/">
    <img 
      src="https://img.shields.io/pypi/v/boto3-client-cache?color=%23FF0000FF&logo=python&label=Latest%20Version"
      alt="pypi_version"
    />
  </a>

  <a href="https://pypi.org/project/boto3-client-cache/">
    <img 
      src="https://img.shields.io/pypi/pyversions/boto3-client-cache?style=pypi&color=%23FF0000FF&logo=python&label=Compatible%20Python%20Versions" 
      alt="py_version"
    />
  </a>

  <a href="https://github.com/michaelthomasletts/boto3-client-cache/actions/workflows/push.yml">
    <img 
      src="https://img.shields.io/github/actions/workflow/status/michaelthomasletts/boto3-client-cache/push.yml?logo=github&color=%23FF0000FF&label=Build" 
      alt="workflow"
    />
  </a>

  <a href="https://github.com/michaelthomasletts/boto3-client-cache/commits/main">
    <img 
      src="https://img.shields.io/github/last-commit/michaelthomasletts/boto3-client-cache?logo=github&color=%23FF0000FF&label=Last%20Commit" 
      alt="last_commit"
    />
  </a>

  <a href="https://michaelthomasletts.com/boto3-client-cache">
    <img 
      src="https://img.shields.io/badge/Official%20Documentation-📘-FF0000?style=flat&labelColor=555&logo=readthedocs" 
      alt="documentation"
    />
  </a>

  <a href="https://github.com/michaelthomasletts/boto3-client-cache">
    <img 
      src="https://img.shields.io/badge/Source%20Code-💻-FF0000?style=flat&labelColor=555&logo=github" 
      alt="github"
    />
  </a>

  <a href="https://github.com/michaelthomasletts/boto3-client-cache/blob/main/LICENSE">
    <img 
      src="https://img.shields.io/static/v1?label=License&message=Apache&color=FF0000&labelColor=555&logo=github&style=flat"
      alt="license"
    />
  </a>

<a href="https://github.com/sponsors/michaelthomasletts">
  <img 
    src="https://img.shields.io/badge/Sponsor%20this%20Project-💙-FF0000?style=flat&labelColor=555&logo=githubsponsors" 
    alt="sponsorship"
  />
</a>

</div>

</br>

## Description

boto3-client-cache provides a concurrency-safe, bounded cache for boto3 clients with deterministic identity semantics.

LRU eviction is supported. LFU eviction is planned for a future release.

## Why this Exists

[boto3 clients consume a large amount of memory](https://github.com/boto/boto3/issues/4568). Many developers never notice this. *At scale*, however, the memory footprint of boto3 clients often becomes clear through manifold consequences. Client caching is an obvious choice for managing multiple clients at scale.

## Design

The most important but challenging design choice for client caching is selecting and enforcing a robust and standardized methodology for unique keys. **boto3-client-cache hashes according to boto3 client signatures**. 

Setting and retrieving clients from the client cache therefore requires an explicit declaration of intention -- that is, *the developer must explicitly pass client initialization parameters to a `ClientCacheKey` object in order to set or retrieve boto3 clients*. This ensures setting and retrieving clients are *unambiguous and deterministic* operations. By locking the client cache, as boto3-client-cache does, race conditions are prevented, enabling developers to confidently employ the client cache at scale with predictable cache eviction behavior. Lastly, by designing the cache like a dict in the standard Python library, the cache is ergonomically familiar and thus easy to use.

These decisions reflect the core design goals of boto3-client-cache: **safety at scale, deterministic behavior, ergonomic interfacing, and explicit identity**.

## Installation

```bash
pip install boto3-client-cache
```

## Quickstart

Refer to the [official documentation](https://michaelthomasletts.com/boto3-client-cache) for additional information.

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

## Error Semantics

Refer to the [official documentation](https://michaelthomasletts.com/boto3-client-cache) for additional information.

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

boto3-client-cache is licensed by the [Apache Software License (2.0)](https://github.com/michaelthomasletts/boto3-client-cache/blob/main/LICENSE).

## Contributing

Refer to the [contributing guidelines](https://github.com/michaelthomasletts/boto3-client-cache?tab=contributing-ov-file) for additional information on how to contribute to boto3-client-cache.

## Special Thanks

- [Patrick Sanders](https://github.com/patricksanders)
