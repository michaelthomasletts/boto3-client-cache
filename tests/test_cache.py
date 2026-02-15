from unittest.mock import MagicMock

import pytest
from botocore.client import BaseClient
from botocore.config import Config

from boto3_client_cache.cache import (
    ClientCache,
    ClientCacheKey,
    LRUClientCache,
    _ClientCacheRegistry,
)
from boto3_client_cache.exceptions import (
    ClientCacheError,
    ClientCacheExistsError,
    ClientCacheNotFoundError,
)


def _client(name: str = "client") -> BaseClient:
    return MagicMock(spec=BaseClient, name=name)


class _TrackingLock:
    def __init__(self) -> None:
        self.enter_count = 0
        self.exit_count = 0

    def __enter__(self) -> "_TrackingLock":
        self.enter_count += 1
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.exit_count += 1
        return False


def test_client_cache_key_equality_hash_str_and_repr() -> None:
    first = ClientCacheKey("s3", region_name="us-east-1")
    second = ClientCacheKey("s3", region_name="us-east-1")
    third = ClientCacheKey("s3", region_name="us-west-2")

    assert first == second
    assert first != third
    assert hash(first) == hash(second)
    assert str(first) == first.label
    assert repr(first) == f"ClientCacheKey(client({first.label}))"


def test_client_cache_key_normalizes_none_and_sorts_kwargs() -> None:
    key = ClientCacheKey(
        "s3",
        None,
        endpoint_url=None,
        region_name="us-east-1",
    )

    # Label preserves full call shape for debugging.
    assert (
        key.label == "'s3', None, endpoint_url=None, region_name='us-east-1'"
    )
    # Key normalizes trailing None args and None kwargs.
    assert key.key == (("s3",), (("region_name", "us-east-1"),))


def test_client_cache_key_freezes_nested_values() -> None:
    key = ClientCacheKey()
    frozen = key._freeze_value(
        {"a": [1, {"b": {3, 2}}], "c": ("x", "y")},
    )

    assert frozen == (
        ("a", (1, (("b", (2, 3)),))),
        ("c", ("x", "y")),
    )


def test_client_cache_key_config_key_is_stable_between_instances() -> None:
    config_one = Config(
        region_name="us-east-1",
        retries={"mode": "standard", "max_attempts": 3},
    )
    config_two = Config(
        retries={"max_attempts": 3, "mode": "standard"},
        region_name="us-east-1",
    )

    positional_one = ClientCacheKey("s3", config_one)
    positional_two = ClientCacheKey("s3", config_two)
    keyword_one = ClientCacheKey("s3", config=config_one)
    keyword_two = ClientCacheKey("s3", config=config_two)

    assert positional_one.key == positional_two.key
    assert keyword_one.key == keyword_two.key
    assert positional_one.key != keyword_one.key
    assert "config=Config(" in keyword_one.label


def test_client_cache_key_config_falls_back_to___dict__when_needed() -> None:
    key = ClientCacheKey()
    config = Config(region_name="us-east-1")
    config._user_provided_options = None  # type: ignore[attr-defined]

    frozen = key._config_cache_key(config)
    formatted = key._format_label_value(config)

    assert isinstance(frozen, tuple)
    assert "Config(" in formatted
    assert "region_name='us-east-1'" in formatted


def test_client_cache_factory_returns_registered_lru_subclass() -> None:
    cache = ClientCache()

    assert isinstance(cache, LRUClientCache)
    assert type(cache) is _ClientCacheRegistry.registry["LRU"]


def test_client_cache_factory_rejects_unsupported_cache_type() -> None:
    with pytest.raises(ClientCacheError, match="Unsupported cache type"):
        ClientCache("LFU")  # type: ignore[call-arg]


def test_lru_cache_init_max_size_and_resizing_with_eviction() -> None:
    cache = LRUClientCache(max_size=-3)
    keys = [ClientCacheKey(f"svc{i}") for i in range(3)]

    for index, key in enumerate(keys):
        cache[key] = _client(f"c{index}")

    assert cache.max_size == 3
    assert tuple(cache.keys()) == tuple(keys)

    cache.max_size = -1
    assert cache.max_size == 1
    assert cache.keys() == (keys[-1],)


def test_lru_cache_call_builds_key_and_inserts_client() -> None:
    cache = LRUClientCache()
    client = _client("s3")

    cache(client, "s3", region_name="us-east-1")

    assert cache[ClientCacheKey("s3", region_name="us-east-1")] is client


def test_lru_cache_string_repr_for_empty_and_populated_cache() -> None:
    cache = LRUClientCache()
    assert str(cache) == "ClientCache(empty)"
    assert repr(cache) == "ClientCache(empty)"

    key = ClientCacheKey("s3", region_name="us-east-1")
    cache[key] = _client("s3")

    rendered = str(cache)
    assert rendered.startswith("ClientCache:\n")
    assert f"RefreshableSession.client({key.label})" in rendered
    assert repr(cache) == rendered


def test_lru_cache_dict_protocol_and_iteration_order() -> None:
    cache = LRUClientCache()
    first = ClientCacheKey("s3")
    second = ClientCacheKey("sns")

    cache[first] = _client("first")
    cache[second] = _client("second")

    assert len(cache) == 2
    assert first in cache
    assert list(iter(cache)) == [first, second]
    assert list(reversed(cache)) == [second, first]


def test_lru_cache_getitem_marks_client_recent_and_miss_raises() -> None:
    cache = LRUClientCache(max_size=2)
    first = ClientCacheKey("s3")
    second = ClientCacheKey("sns")
    third = ClientCacheKey("sqs")

    cache[first] = _client("first")
    cache[second] = _client("second")
    _ = cache[first]  # move first to MRU
    cache[third] = _client("third")

    assert first in cache
    assert third in cache
    assert second not in cache

    with pytest.raises(ClientCacheNotFoundError):
        _ = cache[ClientCacheKey("missing")]


@pytest.mark.parametrize(
    ("key", "obj", "error", "message"),
    [
        ("not-a-key", _client("good"), ClientCacheError, "Cache key must"),
        (ClientCacheKey("s3"), object(), ClientCacheError, "Cache value must"),
    ],
)
def test_lru_cache_setitem_validates_types(
    key: object,
    obj: object,
    error: type[Exception],
    message: str,
) -> None:
    cache = LRUClientCache()
    with pytest.raises(error, match=message):
        cache[key] = obj  # type: ignore[index]


def test_lru_cache_setitem_duplicate_key_raises_exists_error() -> None:
    cache = LRUClientCache()
    key = ClientCacheKey("s3")
    cache[key] = _client("first")

    with pytest.raises(ClientCacheExistsError, match="already exists"):
        cache[key] = _client("second")


def test_lru_cache_delete_and_missing_delete() -> None:
    cache = LRUClientCache()
    key = ClientCacheKey("s3")
    cache[key] = _client("value")

    del cache[key]
    assert key not in cache

    with pytest.raises(ClientCacheNotFoundError, match="Client not found"):
        del cache[key]


def test_lru_cache_keys_values_items_are_tuples_and_snapshots() -> None:
    cache = LRUClientCache()
    first = ClientCacheKey("s3")
    second = ClientCacheKey("sns")
    first_client = _client("first")
    second_client = _client("second")
    cache[first] = first_client
    cache[second] = second_client

    keys_snapshot = cache.keys()
    values_snapshot = cache.values()
    items_snapshot = cache.items()

    assert isinstance(keys_snapshot, tuple)
    assert isinstance(values_snapshot, tuple)
    assert isinstance(items_snapshot, tuple)
    assert keys_snapshot == (first, second)
    assert values_snapshot == (first_client, second_client)
    assert items_snapshot == (
        (first, first_client),
        (second, second_client),
    )

    cache[ClientCacheKey("sqs")] = _client("third")
    assert keys_snapshot == (first, second)
    assert values_snapshot == (first_client, second_client)
    assert items_snapshot == (
        (first, first_client),
        (second, second_client),
    )


def test_lru_cache_get_returns_default_and_marks_hit_recent() -> None:
    cache = LRUClientCache(max_size=2)
    first = ClientCacheKey("s3")
    second = ClientCacheKey("sns")
    third = ClientCacheKey("sqs")
    first_client = _client("first")
    second_client = _client("second")
    default = _client("default")

    cache[first] = first_client
    cache[second] = second_client

    assert cache.get(first) is first_client
    cache[third] = _client("third")
    assert second not in cache
    assert cache.get(ClientCacheKey("missing"), default) is default


def test_lru_cache_pop_and_popitem_and_clear() -> None:
    cache = LRUClientCache()
    first = ClientCacheKey("s3")
    second = ClientCacheKey("sns")
    first_client = _client("first")
    second_client = _client("second")
    cache[first] = first_client
    cache[second] = second_client

    assert cache.pop(first) is first_client
    assert first not in cache

    lru_key, lru_client = cache.popitem()
    assert lru_key == second
    assert lru_client is second_client
    assert len(cache) == 0

    with pytest.raises(ClientCacheNotFoundError, match="No clients found"):
        cache.popitem()

    with pytest.raises(ClientCacheNotFoundError, match="Client not found"):
        cache.pop(first)

    cache[ClientCacheKey("sqs")] = _client("third")
    cache.clear()
    assert len(cache) == 0


def test_lru_cache_copy_is_independent_but_shallow() -> None:
    cache = LRUClientCache(max_size=5)
    key = ClientCacheKey("s3")
    client = _client("original")
    cache[key] = client

    copied = cache.copy()
    assert isinstance(copied, LRUClientCache)
    assert copied is not cache
    assert copied.max_size == cache.max_size
    assert copied[key] is client

    del copied[key]
    assert key in cache


def test_lru_cache_uses_lock_for_core_operations() -> None:
    cache = LRUClientCache()
    lock = _TrackingLock()
    cache._lock = lock  # type: ignore[assignment]
    key = ClientCacheKey("s3")
    client = _client("value")

    cache[key] = client
    _ = cache.get(key)
    _ = cache[key]
    _ = len(cache)
    _ = list(cache)
    _ = key in cache
    _ = cache.keys()
    _ = cache.values()
    _ = cache.items()
    _ = str(cache)
    _ = repr(cache)
    _ = list(reversed(cache))
    cache.max_size = 10
    _ = cache.pop(key)
    cache.clear()

    assert lock.enter_count > 0
    assert lock.enter_count == lock.exit_count


def test_lru_cache_excluded_dict_methods_are_not_supported() -> None:
    cache = LRUClientCache()

    assert not hasattr(cache, "update")
    assert not hasattr(cache, "setdefault")
    assert not hasattr(LRUClientCache, "fromkeys")

    with pytest.raises(TypeError):
        _ = cache | {}  # type: ignore[operator]

    with pytest.raises(TypeError):
        cache |= {}  # type: ignore[operator]
