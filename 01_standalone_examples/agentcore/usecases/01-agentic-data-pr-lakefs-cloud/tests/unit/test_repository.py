import pytest

from orchestrator.repository import (
    NO_REPOS_MESSAGE,
    RepositorySelectionError,
    select_repository,
)
from tests.conftest import FakeLakeFSClient


def test_no_repositories_stops_with_message():
    client = FakeLakeFSClient(repositories=())
    with pytest.raises(RepositorySelectionError) as e:
        select_repository(client)
    assert NO_REPOS_MESSAGE in str(e.value)


def test_single_repository_auto_selected():
    client = FakeLakeFSClient(repositories=("only-repo",))
    assert select_repository(client) == "only-repo"


def test_configured_repository_verified():
    client = FakeLakeFSClient(repositories=("a", "b"))
    assert select_repository(client, configured="b") == "b"


def test_configured_missing_repository_errors():
    client = FakeLakeFSClient(repositories=("a", "b"))
    with pytest.raises(RepositorySelectionError):
        select_repository(client, configured="missing")


def test_multiple_without_chooser_errors():
    client = FakeLakeFSClient(repositories=("a", "b"))
    with pytest.raises(RepositorySelectionError):
        select_repository(client)


def test_multiple_with_chooser():
    client = FakeLakeFSClient(repositories=("a", "b", "c"))
    assert select_repository(client, chooser=lambda ids: ids[1]) == "b"
