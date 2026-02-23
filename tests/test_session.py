from __future__ import annotations

from collections.abc import Callable
from unittest.mock import MagicMock

import boto3
import pytest
from boto3.resources.base import ServiceResource
from botocore.client import BaseClient

import boto3_client_cache as bcc
import boto3_client_cache.session as session_mod
from boto3_client_cache.exceptions import ClientCacheError, ResourceCacheError


@pytest.fixture(autouse=True)
def _restore_default_session() -> None:
    original = boto3.DEFAULT_SESSION
    boto3.DEFAULT_SESSION = None
    try:
        yield
    finally:
        boto3.DEFAULT_SESSION = original


def _mock_client(name: str = "client") -> BaseClient:
    return MagicMock(spec=BaseClient, name=name)


def _mock_resource(name: str = "resource") -> ServiceResource:
    return MagicMock(spec=ServiceResource, name=name)


def test_package_exports_boto3_like_session_helpers() -> None:
    assert bcc.client is session_mod.client
    assert bcc.resource is session_mod.resource
    assert bcc.setup_default_session is session_mod.setup_default_session
    assert "client" in session_mod.__all__
    assert "resource" in session_mod.__all__
    assert "setup_default_session" in session_mod.__all__


def test_setup_default_session_updates_boto3_default_session() -> None:
    session = session_mod.setup_default_session(region_name="us-east-1")

    assert isinstance(session, session_mod.Session)
    assert boto3.DEFAULT_SESSION is session
    assert session_mod._get_default_session() is session


def test_get_default_session_initializes_cached_session_when_missing() -> None:
    assert boto3.DEFAULT_SESSION is None

    session = session_mod._get_default_session()

    assert isinstance(session, session_mod.Session)
    assert boto3.DEFAULT_SESSION is session


def test_module_client_wrapper_allows_service_name_positional_without_max_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_session = MagicMock()
    fake_session.client.return_value = _mock_client("s3")
    monkeypatch.setattr(
        session_mod, "_get_default_session", lambda: fake_session
    )

    result = session_mod.client("s3", region_name="us-east-1")

    assert result is fake_session.client.return_value
    fake_session.client.assert_called_once_with(
        "s3",
        eviction_policy=None,
        max_size=None,
        region_name="us-east-1",
    )


def test_module_resource_wrapper_allows_service_name_positional_without_max_size(  # noqa: E501
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_session = MagicMock()
    fake_session.resource.return_value = _mock_resource("s3")
    monkeypatch.setattr(
        session_mod, "_get_default_session", lambda: fake_session
    )

    result = session_mod.resource("s3", region_name="us-east-1")

    assert result is fake_session.resource.return_value
    fake_session.resource.assert_called_once_with(
        "s3",
        eviction_policy=None,
        max_size=None,
        region_name="us-east-1",
    )


def test_session_client_caches_by_call_signature(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[BaseClient] = []

    def fake_super_client(_self: boto3.Session, *args, **kwargs) -> BaseClient:
        client = _mock_client(f"client_{len(created)}")
        created.append(client)
        return client

    monkeypatch.setattr(boto3.Session, "client", fake_super_client)
    session = session_mod.Session(region_name="us-east-1")

    first = session.client("s3", region_name="us-east-1")
    second = session.client("s3", region_name="us-east-1")
    third = session.client("s3", region_name="us-west-2")

    assert first is second
    assert third is not first
    assert len(created) == 2


def test_session_resource_caches_by_call_signature(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[ServiceResource] = []

    def fake_super_resource(
        _self: boto3.Session, *args, **kwargs
    ) -> ServiceResource:
        resource = _mock_resource(f"resource_{len(created)}")
        created.append(resource)
        return resource

    monkeypatch.setattr(boto3.Session, "resource", fake_super_resource)
    session = session_mod.Session(region_name="us-east-1")

    first = session.resource("s3", region_name="us-east-1")
    second = session.resource("s3", region_name="us-east-1")
    third = session.resource("s3", region_name="us-west-2")

    assert first is second
    assert third is not first
    assert len(created) == 2


@pytest.mark.parametrize(
    ("method_name", "error_type"),
    [
        ("client", ClientCacheError),
        ("resource", ResourceCacheError),
    ],
)
def test_session_methods_validate_eviction_policy(
    method_name: str,
    error_type: type[Exception],
) -> None:
    session = session_mod.Session(region_name="us-east-1")

    method: Callable[..., object] = getattr(session, method_name)

    with pytest.raises(error_type, match="Invalid eviction policy"):
        method("s3", eviction_policy="BAD")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("wrapper_name", "error_type"),
    [
        ("client", ClientCacheError),
        ("resource", ResourceCacheError),
    ],
)
def test_module_wrappers_validate_eviction_policy(
    wrapper_name: str,
    error_type: type[Exception],
) -> None:
    wrapper: Callable[..., object] = getattr(session_mod, wrapper_name)

    with pytest.raises(error_type, match="Invalid eviction policy"):
        wrapper("s3", eviction_policy="BAD")  # type: ignore[arg-type]


def test_session_client_uses_separate_caches_per_eviction_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[BaseClient] = []

    def fake_super_client(_self: boto3.Session, *args, **kwargs) -> BaseClient:
        client = _mock_client(f"client_{len(created)}")
        created.append(client)
        return client

    monkeypatch.setattr(boto3.Session, "client", fake_super_client)
    session = session_mod.Session(region_name="us-east-1")

    default_lru = session.client("s3")
    explicit_lru = session.client("s3", eviction_policy="LRU")
    lfu = session.client("s3", eviction_policy="LFU")

    assert default_lru is explicit_lru
    assert lfu is not default_lru
    assert len(created) == 2


def test_session_resource_uses_separate_caches_per_eviction_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[ServiceResource] = []

    def fake_super_resource(
        _self: boto3.Session, *args, **kwargs
    ) -> ServiceResource:
        resource = _mock_resource(f"resource_{len(created)}")
        created.append(resource)
        return resource

    monkeypatch.setattr(boto3.Session, "resource", fake_super_resource)
    session = session_mod.Session(region_name="us-east-1")

    default_lru = session.resource("s3")
    explicit_lru = session.resource("s3", eviction_policy="LRU")
    lfu = session.resource("s3", eviction_policy="LFU")

    assert default_lru is explicit_lru
    assert lfu is not default_lru
    assert len(created) == 2


def test_session_client_updates_cache_max_size_when_provided(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        boto3.Session,
        "client",
        lambda _self, *args, **kwargs: _mock_client("client"),
    )
    session = session_mod.Session(region_name="us-east-1")

    session.client("s3", max_size=3)
    assert session.cache["client"]["LRU"].max_size == 3

    session.client("s3")
    assert session.cache["client"]["LRU"].max_size == 3

    session.client("s3", max_size=1)
    assert session.cache["client"]["LRU"].max_size == 1


def test_session_resource_updates_cache_max_size_when_provided(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        boto3.Session,
        "resource",
        lambda _self, *args, **kwargs: _mock_resource("resource"),
    )
    session = session_mod.Session(region_name="us-east-1")

    session.resource("s3", max_size=4)
    assert session.cache["resource"]["LRU"].max_size == 4

    session.resource("s3")
    assert session.cache["resource"]["LRU"].max_size == 4

    session.resource("s3", max_size=2)
    assert session.cache["resource"]["LRU"].max_size == 2


def test_session_caches_are_not_shared_across_session_instances() -> None:
    first = session_mod.Session(region_name="us-east-1")
    second = session_mod.Session(region_name="us-west-2")

    assert first.cache is not second.cache
    assert first.cache.client is not second.cache.client
    assert first.cache.resource is not second.cache.resource
    assert first.cache.client["LRU"] is not second.cache.client["LRU"]
    assert first.cache.client["LFU"] is not second.cache.client["LFU"]
    assert first.cache.resource["LRU"] is not second.cache.resource["LRU"]
    assert first.cache.resource["LFU"] is not second.cache.resource["LFU"]
