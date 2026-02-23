from unittest.mock import MagicMock

import pytest
from botocore.client import BaseClient
from botocore.config import Config

from boto3_client_cache.cache import (
    ClientCache,
    ClientCacheKey,
    LFUClientCache,
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


def test_client_cache_key_moves_service_name_kwarg_into_args() -> None:
    positional = ClientCacheKey("s3", region_name="us-east-1")
    keyword = ClientCacheKey(service_name="s3", region_name="us-east-1")

    assert keyword == positional
    assert hash(keyword) == hash(positional)
    assert keyword.key == positional.key
    assert keyword._key == positional._key
    assert keyword.label == positional.label
    assert keyword._label == positional._label
    assert "service_name=" not in keyword.label
    assert "service_name=" not in keyword._label
    assert "service_name" not in dict(keyword.key[1])
    assert "service_name" not in dict(keyword._key[1])


def test_client_cache_key_removes_session_cache_control_kwargs() -> None:
    plain = ClientCacheKey("s3", region_name="us-east-1")
    with_controls = ClientCacheKey(
        "s3",
        region_name="us-east-1",
        eviction_policy="LFU",
        max_size=123,
    )

    assert with_controls == plain
    assert with_controls.key == plain.key
    assert with_controls._key == plain._key
    assert with_controls.label == plain.label
    assert with_controls._label == plain._label
    assert "eviction_policy=" not in with_controls.label
    assert "eviction_policy=" not in with_controls._label
    assert "max_size=" not in with_controls.label
    assert "max_size=" not in with_controls._label
    assert "eviction_policy" not in dict(with_controls.key[1])
    assert "eviction_policy" not in dict(with_controls._key[1])
    assert "max_size" not in dict(with_controls.key[1])
    assert "max_size" not in dict(with_controls._key[1])


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


@pytest.mark.parametrize(
    ("sensitive_key", "value"),
    [
        ("aws_access_key_id", "AKIA_TEST_KEY"),
        ("aws_secret_access_key", "SECRET_TEST_KEY"),
        ("aws_session_token", "SESSION_TEST_TOKEN"),
    ],
)
def test_client_cache_key_obscures_sensitive_keyword_values(
    sensitive_key: str,
    value: str,
) -> None:
    key = ClientCacheKey(
        "s3", region_name="us-east-1", **{sensitive_key: value}
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
def test_client_cache_key_obscures_sensitive_positional_values(
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
    key = ClientCacheKey(*args)

    assert value not in key.label
    assert value in key._label
    assert key.key[0][arg_position] == "***"
    assert key._key[0][arg_position] == value


def test_client_cache_key_print_and_repr_do_not_expose_sensitive_data(
    capsys: pytest.CaptureFixture[str],
) -> None:
    key = ClientCacheKey(
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


def test_client_cache_factory_returns_registered_lru_subclass() -> None:
    cache = ClientCache()

    assert isinstance(cache, LRUClientCache)
    assert type(cache) is _ClientCacheRegistry.registry["LRU"]


def test_client_cache_factory_returns_registered_lfu_subclass() -> None:
    cache = ClientCache("LFU")

    assert isinstance(cache, LFUClientCache)
    assert type(cache) is _ClientCacheRegistry.registry["LFU"]


def test_client_cache_factory_rejects_unsupported_eviction_policy() -> None:
    with pytest.raises(ClientCacheError, match="Unsupported cache type"):
        ClientCache("FIFO")  # type: ignore[call-arg]


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


def test_lfu_cache_init_max_size_and_resizing_with_eviction() -> None:
    cache = LFUClientCache(max_size=-3)
    keys = [ClientCacheKey(f"svc{i}") for i in range(3)]

    for index, key in enumerate(keys):
        cache[key] = _client(f"c{index}")

    assert cache.max_size == 3
    assert cache.keys() == tuple(keys)

    _ = cache[keys[0]]
    cache.max_size = -1
    assert cache.max_size == 1
    assert cache.keys() == (keys[0],)


def test_lfu_cache_string_repr_for_empty_and_populated_cache() -> None:
    cache = LFUClientCache()
    assert str(cache) == "ClientCache(empty)"
    assert repr(cache) == "ClientCache(empty)"

    key = ClientCacheKey("s3", region_name="us-east-1")
    cache[key] = _client("s3")

    rendered = str(cache)
    assert rendered.startswith("ClientCache:\n")
    assert f"RefreshableSession.client({key.label})" in rendered
    assert repr(cache) == rendered


def test_lfu_cache_dict_protocol_and_iteration_order() -> None:
    cache = LFUClientCache()
    first = ClientCacheKey("s3")
    second = ClientCacheKey("sns")

    cache[first] = _client("first")
    cache[second] = _client("second")
    _ = cache[first]

    assert len(cache) == 2
    assert first in cache
    assert list(iter(cache)) == [second, first]
    assert list(reversed(cache)) == [first, second]


def test_lfu_cache_getitem_marks_client_frequent_and_miss_raises() -> None:
    cache = LFUClientCache(max_size=2)
    first = ClientCacheKey("s3")
    second = ClientCacheKey("sns")
    third = ClientCacheKey("sqs")

    cache[first] = _client("first")
    cache[second] = _client("second")
    _ = cache[first]
    cache[third] = _client("third")

    assert first in cache
    assert third in cache
    assert second not in cache

    with pytest.raises(ClientCacheNotFoundError):
        _ = cache[ClientCacheKey("missing")]


def test_lfu_cache_tie_breaker_eviction_is_lru_within_frequency() -> None:
    cache = LFUClientCache(max_size=2)
    first = ClientCacheKey("s3")
    second = ClientCacheKey("sns")
    third = ClientCacheKey("sqs")

    cache[first] = _client("first")
    cache[second] = _client("second")
    cache[third] = _client("third")

    assert first not in cache
    assert second in cache
    assert third in cache


@pytest.mark.parametrize(
    ("key", "obj", "error", "message"),
    [
        ("not-a-key", _client("good"), ClientCacheError, "Cache key must"),
        (ClientCacheKey("s3"), object(), ClientCacheError, "Cache value must"),
    ],
)
def test_lfu_cache_setitem_validates_types(
    key: object,
    obj: object,
    error: type[Exception],
    message: str,
) -> None:
    cache = LFUClientCache()
    with pytest.raises(error, match=message):
        cache[key] = obj  # type: ignore[index]


def test_lfu_cache_setitem_duplicate_key_raises_exists_error() -> None:
    cache = LFUClientCache()
    key = ClientCacheKey("s3")
    cache[key] = _client("first")

    with pytest.raises(ClientCacheExistsError, match="already exists"):
        cache[key] = _client("second")


def test_lfu_cache_delete_and_missing_delete() -> None:
    cache = LFUClientCache()
    key = ClientCacheKey("s3")
    cache[key] = _client("value")

    del cache[key]
    assert key not in cache

    with pytest.raises(ClientCacheNotFoundError, match="Client not found"):
        del cache[key]


def test_lfu_cache_keys_values_items_are_tuples_and_snapshots() -> None:
    cache = LFUClientCache()
    first = ClientCacheKey("s3")
    second = ClientCacheKey("sns")
    first_client = _client("first")
    second_client = _client("second")
    cache[first] = first_client
    cache[second] = second_client
    _ = cache[first]

    keys_snapshot = cache.keys()
    values_snapshot = cache.values()
    items_snapshot = cache.items()

    assert isinstance(keys_snapshot, tuple)
    assert isinstance(values_snapshot, tuple)
    assert isinstance(items_snapshot, tuple)
    assert keys_snapshot == (second, first)
    assert values_snapshot == (second_client, first_client)
    assert items_snapshot == (
        (second, second_client),
        (first, first_client),
    )

    cache[ClientCacheKey("sqs")] = _client("third")
    assert keys_snapshot == (second, first)
    assert values_snapshot == (second_client, first_client)
    assert items_snapshot == (
        (second, second_client),
        (first, first_client),
    )


def test_lfu_cache_get_returns_default_and_marks_hit_frequent() -> None:
    cache = LFUClientCache(max_size=2)
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


def test_lfu_cache_pop_and_popitem_and_clear() -> None:
    cache = LFUClientCache()
    first = ClientCacheKey("s3")
    second = ClientCacheKey("sns")
    first_client = _client("first")
    second_client = _client("second")
    cache[first] = first_client
    cache[second] = second_client

    assert cache.pop(first) is first_client
    assert first not in cache

    lfu_key, lfu_client = cache.popitem()
    assert lfu_key == second
    assert lfu_client is second_client
    assert len(cache) == 0

    with pytest.raises(ClientCacheNotFoundError, match="No clients found"):
        cache.popitem()

    with pytest.raises(ClientCacheNotFoundError, match="Client not found"):
        cache.pop(first)

    cache[ClientCacheKey("sqs")] = _client("third")
    cache.clear()
    assert len(cache) == 0


def test_lfu_cache_copy_is_independent_but_shallow() -> None:
    cache = LFUClientCache(max_size=2)
    first = ClientCacheKey("s3")
    second = ClientCacheKey("sns")
    third = ClientCacheKey("sqs")
    first_client = _client("first")
    second_client = _client("second")
    cache[first] = first_client
    cache[second] = second_client
    _ = cache[first]

    copied = cache.copy()
    assert isinstance(copied, LFUClientCache)
    assert copied is not cache
    assert copied.max_size == cache.max_size
    assert copied[first] is first_client
    assert copied[second] is second_client

    copied[third] = _client("third")
    assert second not in copied
    assert second in cache


def test_lfu_cache_uses_lock_for_core_operations() -> None:
    cache = LFUClientCache()
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


def test_lfu_cache_excluded_dict_methods_are_not_supported() -> None:
    cache = LFUClientCache()

    assert not hasattr(cache, "update")
    assert not hasattr(cache, "setdefault")
    assert not hasattr(LFUClientCache, "fromkeys")

    with pytest.raises(TypeError):
        _ = cache | {}  # type: ignore[operator]

    with pytest.raises(TypeError):
        cache |= {}  # type: ignore[operator]
