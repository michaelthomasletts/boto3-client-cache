from unittest.mock import MagicMock

import pytest
from boto3.resources.base import ServiceResource
from botocore.config import Config

from boto3_client_cache.cache import (
    LFUResourceCache,
    LRUResourceCache,
    ResourceCache,
    ResourceCacheKey,
    _ResourceCacheRegistry,
)
from boto3_client_cache.exceptions import (
    ResourceCacheError,
    ResourceCacheExistsError,
    ResourceCacheNotFoundError,
)


def _resource(name: str = "client") -> ServiceResource:
    return MagicMock(spec=ServiceResource, name=name)


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


def test_resource_cache_key_equality_hash_str_and_repr() -> None:
    first = ResourceCacheKey("s3", region_name="us-east-1")
    second = ResourceCacheKey("s3", region_name="us-east-1")
    third = ResourceCacheKey("s3", region_name="us-west-2")

    assert first == second
    assert first != third
    assert hash(first) == hash(second)
    assert str(first) == first.label
    assert repr(first) == f"ResourceCacheKey(resource({first.label}))"


def test_resource_cache_key_normalizes_none_and_sorts_kwargs() -> None:
    key = ResourceCacheKey(
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


def test_resource_cache_key_freezes_nested_values() -> None:
    key = ResourceCacheKey()
    frozen = key._freeze_value(
        {"a": [1, {"b": {3, 2}}], "c": ("x", "y")},
    )

    assert frozen == (
        ("a", (1, (("b", (2, 3)),))),
        ("c", ("x", "y")),
    )


def test_resource_cache_key_config_key_is_stable_between_instances() -> None:
    config_one = Config(
        region_name="us-east-1",
        retries={"mode": "standard", "max_attempts": 3},
    )
    config_two = Config(
        retries={"max_attempts": 3, "mode": "standard"},
        region_name="us-east-1",
    )

    positional_one = ResourceCacheKey("s3", config_one)
    positional_two = ResourceCacheKey("s3", config_two)
    keyword_one = ResourceCacheKey("s3", config=config_one)
    keyword_two = ResourceCacheKey("s3", config=config_two)

    assert positional_one.key == positional_two.key
    assert keyword_one.key == keyword_two.key
    assert positional_one.key != keyword_one.key
    assert "config=Config(" in keyword_one.label


def test_resource_cache_key_config_falls_back_to___dict__when_needed() -> None:
    key = ResourceCacheKey()
    config = Config(region_name="us-east-1")
    config._user_provided_options = None  # type: ignore[attr-defined]

    frozen = key._config_cache_key(config)
    formatted = key._format_label_value(config)

    assert isinstance(frozen, tuple)
    assert "Config(" in formatted
    assert "region_name='us-east-1'" in formatted


@pytest.mark.parametrize(
    ("sensitive_key", "value"),
    [
        ("aws_access_key_id", "AKIA_TEST_KEY"),
        ("aws_secret_access_key", "SECRET_TEST_KEY"),
        ("aws_session_token", "SESSION_TEST_TOKEN"),
    ],
)
def test_resource_cache_key_obscures_sensitive_keyword_values(
    sensitive_key: str,
    value: str,
) -> None:
    key = ResourceCacheKey(
        "s3",
        region_name="us-east-1",
        **{sensitive_key: value},
    )

    assert f"{sensitive_key}=***" in key.label
    assert value not in key.label
    assert f"{sensitive_key}={value!r}" in key._label

    public_kwargs = dict(key.key[1])
    private_kwargs = dict(key._key[1])

    assert public_kwargs[sensitive_key] == "***"
    assert private_kwargs[sensitive_key] == value
    assert public_kwargs["region_name"] == "us-east-1"
    assert private_kwargs["region_name"] == "us-east-1"


@pytest.mark.parametrize(
    ("arg_position", "value"),
    [
        (6, "AKIA_TEST_KEY"),
        (7, "SECRET_TEST_KEY"),
        (8, "SESSION_TEST_TOKEN"),
    ],
)
def test_resource_cache_key_obscures_sensitive_positional_values(
    arg_position: int,
    value: str,
) -> None:
    args: list[object | None] = [
        "s3",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ]
    args[arg_position] = value
    key = ResourceCacheKey(*args)

    assert value not in key.label
    assert value in key._label
    assert key.key[0][arg_position] == "***"
    assert key._key[0][arg_position] == value


def test_resource_cache_key_print_and_repr_do_not_expose_sensitive_data(
    capsys: pytest.CaptureFixture[str],
) -> None:
    key = ResourceCacheKey(
        "s3",
        aws_access_key_id="AKIA_TEST_KEY",
        aws_secret_access_key="SECRET_TEST_KEY",
        aws_session_token="SESSION_TEST_TOKEN",
    )

    print(key)
    print([key])
    captured = capsys.readouterr().out

    assert "AKIA_TEST_KEY" not in captured
    assert "SECRET_TEST_KEY" not in captured
    assert "SESSION_TEST_TOKEN" not in captured
    assert "aws_access_key_id=***" in captured
    assert "aws_secret_access_key=***" in captured
    assert "aws_session_token=***" in captured

    rendered = repr(key)
    assert "AKIA_TEST_KEY" not in rendered
    assert "SECRET_TEST_KEY" not in rendered
    assert "SESSION_TEST_TOKEN" not in rendered
    assert "aws_access_key_id=***" in rendered
    assert "aws_secret_access_key=***" in rendered
    assert "aws_session_token=***" in rendered


def test_resource_cache_factory_returns_registered_lru_subclass() -> None:
    cache = ResourceCache()

    assert isinstance(cache, LRUResourceCache)
    assert type(cache) is _ResourceCacheRegistry.registry["LRU"]


def test_resource_cache_factory_returns_registered_lfu_subclass() -> None:
    cache = ResourceCache("LFU")

    assert isinstance(cache, LFUResourceCache)
    assert type(cache) is _ResourceCacheRegistry.registry["LFU"]


def test_resource_cache_factory_rejects_unsupported_eviction_policy() -> None:
    with pytest.raises(ResourceCacheError, match="Unsupported cache type"):
        ResourceCache("FIFO")  # type: ignore[call-arg]


def test_lru_cache_init_max_size_and_resizing_with_eviction() -> None:
    cache = LRUResourceCache(max_size=-3)
    keys = [ResourceCacheKey(f"svc{i}") for i in range(3)]

    for index, key in enumerate(keys):
        cache[key] = _resource(f"c{index}")

    assert cache.max_size == 3
    assert tuple(cache.keys()) == tuple(keys)

    cache.max_size = -1
    assert cache.max_size == 1
    assert cache.keys() == (keys[-1],)


def test_lru_cache_string_repr_for_empty_and_populated_cache() -> None:
    cache = LRUResourceCache()
    assert str(cache) == "ResourceCache(empty)"
    assert repr(cache) == "ResourceCache(empty)"

    key = ResourceCacheKey("s3", region_name="us-east-1")
    cache[key] = _resource("s3")

    rendered = str(cache)
    assert rendered.startswith("ResourceCache:\n")
    assert f"RefreshableSession.resource({key.label})" in rendered
    assert repr(cache) == rendered


def test_lru_cache_dict_protocol_and_iteration_order() -> None:
    cache = LRUResourceCache()
    first = ResourceCacheKey("s3")
    second = ResourceCacheKey("sns")

    cache[first] = _resource("first")
    cache[second] = _resource("second")

    assert len(cache) == 2
    assert first in cache
    assert list(iter(cache)) == [first, second]
    assert list(reversed(cache)) == [second, first]


def test_lru_cache_getitem_marks_client_recent_and_miss_raises() -> None:
    cache = LRUResourceCache(max_size=2)
    first = ResourceCacheKey("s3")
    second = ResourceCacheKey("sns")
    third = ResourceCacheKey("sqs")

    cache[first] = _resource("first")
    cache[second] = _resource("second")
    _ = cache[first]  # move first to MRU
    cache[third] = _resource("third")

    assert first in cache
    assert third in cache
    assert second not in cache

    with pytest.raises(ResourceCacheNotFoundError):
        _ = cache[ResourceCacheKey("missing")]


@pytest.mark.parametrize(
    ("key", "obj", "error", "message"),
    [
        ("not-a-key", _resource("good"), ResourceCacheError, "Cache key must"),
        (
            ResourceCacheKey("s3"),
            object(),
            ResourceCacheError,
            "Cache value must",
        ),
    ],
)
def test_lru_cache_setitem_validates_types(
    key: object,
    obj: object,
    error: type[Exception],
    message: str,
) -> None:
    cache = LRUResourceCache()
    with pytest.raises(error, match=message):
        cache[key] = obj  # type: ignore[index]


def test_lru_cache_setitem_duplicate_key_raises_exists_error() -> None:
    cache = LRUResourceCache()
    key = ResourceCacheKey("s3")
    cache[key] = _resource("first")

    with pytest.raises(ResourceCacheExistsError, match="already exists"):
        cache[key] = _resource("second")


def test_lru_cache_delete_and_missing_delete() -> None:
    cache = LRUResourceCache()
    key = ResourceCacheKey("s3")
    cache[key] = _resource("value")

    del cache[key]
    assert key not in cache

    with pytest.raises(ResourceCacheNotFoundError, match="Client not found"):
        del cache[key]


def test_lru_cache_keys_values_items_are_tuples_and_snapshots() -> None:
    cache = LRUResourceCache()
    first = ResourceCacheKey("s3")
    second = ResourceCacheKey("sns")
    first_resource = _resource("first")
    second_resource = _resource("second")
    cache[first] = first_resource
    cache[second] = second_resource

    keys_snapshot = cache.keys()
    values_snapshot = cache.values()
    items_snapshot = cache.items()

    assert isinstance(keys_snapshot, tuple)
    assert isinstance(values_snapshot, tuple)
    assert isinstance(items_snapshot, tuple)
    assert keys_snapshot == (first, second)
    assert values_snapshot == (first_resource, second_resource)
    assert items_snapshot == (
        (first, first_resource),
        (second, second_resource),
    )

    cache[ResourceCacheKey("sqs")] = _resource("third")
    assert keys_snapshot == (first, second)
    assert values_snapshot == (first_resource, second_resource)
    assert items_snapshot == (
        (first, first_resource),
        (second, second_resource),
    )


def test_lru_cache_get_returns_default_and_marks_hit_recent() -> None:
    cache = LRUResourceCache(max_size=2)
    first = ResourceCacheKey("s3")
    second = ResourceCacheKey("sns")
    third = ResourceCacheKey("sqs")
    first_resource = _resource("first")
    second_resource = _resource("second")
    default = _resource("default")

    cache[first] = first_resource
    cache[second] = second_resource

    assert cache.get(first) is first_resource
    cache[third] = _resource("third")
    assert second not in cache
    assert cache.get(ResourceCacheKey("missing"), default) is default


def test_lru_cache_pop_and_popitem_and_clear() -> None:
    cache = LRUResourceCache()
    first = ResourceCacheKey("s3")
    second = ResourceCacheKey("sns")
    first_resource = _resource("first")
    second_resource = _resource("second")
    cache[first] = first_resource
    cache[second] = second_resource

    assert cache.pop(first) is first_resource
    assert first not in cache

    lru_key, lru_resource = cache.popitem()
    assert lru_key == second
    assert lru_resource is second_resource
    assert len(cache) == 0

    with pytest.raises(ResourceCacheNotFoundError, match="No clients found"):
        cache.popitem()

    with pytest.raises(ResourceCacheNotFoundError, match="Client not found"):
        cache.pop(first)

    cache[ResourceCacheKey("sqs")] = _resource("third")
    cache.clear()
    assert len(cache) == 0


def test_lru_cache_copy_is_independent_but_shallow() -> None:
    cache = LRUResourceCache(max_size=5)
    key = ResourceCacheKey("s3")
    client = _resource("original")
    cache[key] = client

    copied = cache.copy()
    assert isinstance(copied, LRUResourceCache)
    assert copied is not cache
    assert copied.max_size == cache.max_size
    assert copied[key] is client

    del copied[key]
    assert key in cache


def test_lru_cache_uses_lock_for_core_operations() -> None:
    cache = LRUResourceCache()
    lock = _TrackingLock()
    cache._lock = lock  # type: ignore[assignment]
    key = ResourceCacheKey("s3")
    client = _resource("value")

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
    cache = LRUResourceCache()

    assert not hasattr(cache, "update")
    assert not hasattr(cache, "setdefault")
    assert not hasattr(LRUResourceCache, "fromkeys")

    with pytest.raises(TypeError):
        _ = cache | {}  # type: ignore[operator]

    with pytest.raises(TypeError):
        cache |= {}  # type: ignore[operator]


def test_lfu_cache_init_max_size_and_resizing_with_eviction() -> None:
    cache = LFUResourceCache(max_size=-3)
    keys = [ResourceCacheKey(f"svc{i}") for i in range(3)]

    for index, key in enumerate(keys):
        cache[key] = _resource(f"c{index}")

    assert cache.max_size == 3
    assert cache.keys() == tuple(keys)

    _ = cache[keys[0]]
    cache.max_size = -1
    assert cache.max_size == 1
    assert cache.keys() == (keys[0],)


def test_lfu_cache_string_repr_for_empty_and_populated_cache() -> None:
    cache = LFUResourceCache()
    assert str(cache) == "ResourceCache(empty)"
    assert repr(cache) == "ResourceCache(empty)"

    key = ResourceCacheKey("s3", region_name="us-east-1")
    cache[key] = _resource("s3")

    rendered = str(cache)
    assert rendered.startswith("ResourceCache:\n")
    assert f"RefreshableSession.resource({key.label})" in rendered
    assert repr(cache) == rendered


def test_lfu_cache_dict_protocol_and_iteration_order() -> None:
    cache = LFUResourceCache()
    first = ResourceCacheKey("s3")
    second = ResourceCacheKey("sns")

    cache[first] = _resource("first")
    cache[second] = _resource("second")
    _ = cache[first]

    assert len(cache) == 2
    assert first in cache
    assert list(iter(cache)) == [second, first]
    assert list(reversed(cache)) == [first, second]


def test_lfu_cache_getitem_marks_client_frequent_and_miss_raises() -> None:
    cache = LFUResourceCache(max_size=2)
    first = ResourceCacheKey("s3")
    second = ResourceCacheKey("sns")
    third = ResourceCacheKey("sqs")

    cache[first] = _resource("first")
    cache[second] = _resource("second")
    _ = cache[first]
    cache[third] = _resource("third")

    assert first in cache
    assert third in cache
    assert second not in cache

    with pytest.raises(ResourceCacheNotFoundError):
        _ = cache[ResourceCacheKey("missing")]


def test_lfu_cache_tie_breaker_eviction_is_lru_within_frequency() -> None:
    cache = LFUResourceCache(max_size=2)
    first = ResourceCacheKey("s3")
    second = ResourceCacheKey("sns")
    third = ResourceCacheKey("sqs")

    cache[first] = _resource("first")
    cache[second] = _resource("second")
    cache[third] = _resource("third")

    assert first not in cache
    assert second in cache
    assert third in cache


@pytest.mark.parametrize(
    ("key", "obj", "error", "message"),
    [
        ("not-a-key", _resource("good"), ResourceCacheError, "Cache key must"),
        (
            ResourceCacheKey("s3"),
            object(),
            ResourceCacheError,
            "Cache value must",
        ),
    ],
)
def test_lfu_cache_setitem_validates_types(
    key: object,
    obj: object,
    error: type[Exception],
    message: str,
) -> None:
    cache = LFUResourceCache()
    with pytest.raises(error, match=message):
        cache[key] = obj  # type: ignore[index]


def test_lfu_cache_setitem_duplicate_key_raises_exists_error() -> None:
    cache = LFUResourceCache()
    key = ResourceCacheKey("s3")
    cache[key] = _resource("first")

    with pytest.raises(ResourceCacheExistsError, match="already exists"):
        cache[key] = _resource("second")


def test_lfu_cache_delete_and_missing_delete() -> None:
    cache = LFUResourceCache()
    key = ResourceCacheKey("s3")
    cache[key] = _resource("value")

    del cache[key]
    assert key not in cache

    with pytest.raises(ResourceCacheNotFoundError, match="Client not found"):
        del cache[key]


def test_lfu_cache_keys_values_items_are_tuples_and_snapshots() -> None:
    cache = LFUResourceCache()
    first = ResourceCacheKey("s3")
    second = ResourceCacheKey("sns")
    first_resource = _resource("first")
    second_resource = _resource("second")
    cache[first] = first_resource
    cache[second] = second_resource
    _ = cache[first]

    keys_snapshot = cache.keys()
    values_snapshot = cache.values()
    items_snapshot = cache.items()

    assert isinstance(keys_snapshot, tuple)
    assert isinstance(values_snapshot, tuple)
    assert isinstance(items_snapshot, tuple)
    assert keys_snapshot == (second, first)
    assert values_snapshot == (second_resource, first_resource)
    assert items_snapshot == (
        (second, second_resource),
        (first, first_resource),
    )

    cache[ResourceCacheKey("sqs")] = _resource("third")
    assert keys_snapshot == (second, first)
    assert values_snapshot == (second_resource, first_resource)
    assert items_snapshot == (
        (second, second_resource),
        (first, first_resource),
    )


def test_lfu_cache_get_returns_default_and_marks_hit_frequent() -> None:
    cache = LFUResourceCache(max_size=2)
    first = ResourceCacheKey("s3")
    second = ResourceCacheKey("sns")
    third = ResourceCacheKey("sqs")
    first_resource = _resource("first")
    second_resource = _resource("second")
    default = _resource("default")

    cache[first] = first_resource
    cache[second] = second_resource

    assert cache.get(first) is first_resource
    cache[third] = _resource("third")
    assert second not in cache
    assert cache.get(ResourceCacheKey("missing"), default) is default


def test_lfu_cache_pop_and_popitem_and_clear() -> None:
    cache = LFUResourceCache()
    first = ResourceCacheKey("s3")
    second = ResourceCacheKey("sns")
    first_resource = _resource("first")
    second_resource = _resource("second")
    cache[first] = first_resource
    cache[second] = second_resource

    assert cache.pop(first) is first_resource
    assert first not in cache

    lfu_key, lfu_resource = cache.popitem()
    assert lfu_key == second
    assert lfu_resource is second_resource
    assert len(cache) == 0

    with pytest.raises(ResourceCacheNotFoundError, match="No clients found"):
        cache.popitem()

    with pytest.raises(ResourceCacheNotFoundError, match="Client not found"):
        cache.pop(first)

    cache[ResourceCacheKey("sqs")] = _resource("third")
    cache.clear()
    assert len(cache) == 0


def test_lfu_cache_copy_is_independent_but_shallow() -> None:
    cache = LFUResourceCache(max_size=2)
    first = ResourceCacheKey("s3")
    second = ResourceCacheKey("sns")
    third = ResourceCacheKey("sqs")
    first_resource = _resource("first")
    second_resource = _resource("second")
    cache[first] = first_resource
    cache[second] = second_resource
    _ = cache[first]

    copied = cache.copy()
    assert isinstance(copied, LFUResourceCache)
    assert copied is not cache
    assert copied.max_size == cache.max_size
    assert copied[first] is first_resource
    assert copied[second] is second_resource

    copied[third] = _resource("third")
    assert second not in copied
    assert second in cache


def test_lfu_cache_uses_lock_for_core_operations() -> None:
    cache = LFUResourceCache()
    lock = _TrackingLock()
    cache._lock = lock  # type: ignore[assignment]
    key = ResourceCacheKey("s3")
    client = _resource("value")

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


def test_lfu_cache_excluded_dict_methods_are_not_supported() -> None:
    cache = LFUResourceCache()

    assert not hasattr(cache, "update")
    assert not hasattr(cache, "setdefault")
    assert not hasattr(LFUResourceCache, "fromkeys")

    with pytest.raises(TypeError):
        _ = cache | {}  # type: ignore[operator]

    with pytest.raises(TypeError):
        cache |= {}  # type: ignore[operator]
