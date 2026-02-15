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

  <a href="https://github.com/michaelthomasletts/boto3-client-cache/stargazers">
    <img 
      src="https://img.shields.io/github/stars/michaelthomasletts/boto3-client-cache?style=flat&logo=github&labelColor=555&color=FF0000&label=Stars" 
      alt="stars"
    />
  </a>

<a href="https://pepy.tech/projects/boto3-client-cache">
  <img
    src="https://img.shields.io/endpoint?url=https%3A%2F%2Fmichaelthomasletts.github.io%2Fpepy-stats%2Fboto3-client-cache.json&style=flat&logo=python&labelColor=555&color=FF0000"
    alt="downloads"
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
      src="https://img.shields.io/static/v1?label=License&message=MPL-2.0&color=FF0000&labelColor=555&logo=github&style=flat"
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

A simple Python package which caches boto3 clients. 

Includes LRU eviction. LFU eviction will be included in a future release.

## Raison d'Être

**boto3 clients consume a large amount of memory**. Many developers never notice. *At scale*, however, this becomes painfully obvious. There is a clear incentive, therefore, to avoid initializing duplicate client objects. Client caching is an obvious solution.

The most challenging aspect of boto3 client caching is selecting robust and standardized unique keys. Managing ad-hoc keys at scale is unwieldy and insecure. **boto3-client-cache hashes according to client signatures**. Setting and retrieving clients from the client cache therefore requires an explicit declaration of intention -- that is, *the developer must explicitly pass client initialization parameters to a `CacheKey` object in order to set or retrieve a client*.

From a developer experience perspective, this design - that is, forcing `CacheKey` - may feel clunky; however, it ensures setting and retrieving clients are unambiguous operations. Further, locking the client cache, as boto3-client-cache does, prevents race conditions, enabling developers to confidently employ the client cache at scale.

Although boto3-client-cache can help any developer working with the AWS Python SDK at any scale, it was designed primarily for security, cloud, machine learning, and platform teams operating at scale. 

boto3-client-cache, it should be noted, is also a critical dependency for [boto3-refresh-session](https://github.com/michaelthomasletts/boto3-refresh-session).

## Installation

```bash
pip install boto3-client-cache
```

## Quickstart

```python
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

# this raises a ClientCacheExistsError
cache[key] = boto3.client(**kwargs)

# this raises a ClientCacheNotFoundError
cache[ClientCacheKey(service_name="ec2", region_name="us-west-2")]

# but this returns None instead of raising ClientCacheNotFoundError
cache.get(ClientCacheKey(service_name="ec2", region_name="us-west-2"))

# this raises a ClientCacheError
cache["this is not a ClientCacheKey"]

# and this raises a ClientCacheError
cache[ClientCacheKey("s3")] = "this is not a boto3 client"
```

## License

boto3-client-cache is licensed by the [Apache Software License (2.0)](https://github.com/michaelthomasletts/boto3-client-cache/blob/main/LICENSE).

## Special Thanks

- [Patrick Sanders](https://github.com/patricksanders)
