import json
import shutil
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest
import requests
import zstandard

from conda_oci_mirror import defaults, util
from conda_oci_mirror.mirror import Mirror
from conda_oci_mirror.oras import Pusher, Registry, oras
from conda_oci_mirror.package import Package
from conda_oci_mirror.repo import PackageRepo, RepoData
from conda_oci_mirror.tasks import TaskBase


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def no_network(*args, **kwargs):
        raise AssertionError("Unexpected network request")

    monkeypatch.setattr("requests.sessions.Session.request", no_network)
    monkeypatch.setattr(TaskBase, "wait", lambda *args: None)
    monkeypatch.setattr("conda_oci_mirror.tasks.time.sleep", lambda seconds: None)


@pytest.mark.parametrize("serial", [False, True])
@pytest.mark.parametrize("fail", [False, True])
@pytest.mark.parametrize("dry_run", [False, True])
def test_metadata_waits_for_all_packages(monkeypatch, tmp_path, serial, fail, dry_run):
    mirror = Mirror(
        "test",
        [],
        subdirs=["linux-64", "osx-64"],
        registry="example.com/test",
        cache_dir=tmp_path,
    )
    monkeypatch.setattr(
        PackageRepo, "find_packages", lambda *args, **kwargs: [("demo-1.0-0.conda", {})]
    )
    monkeypatch.setattr(Package, "ensure_file", lambda self: None)
    monkeypatch.setattr(Package, "delete", lambda self: None)
    completed = set()
    published = []

    def upload_package(package, dry_run=False):
        if fail:
            raise RuntimeError("upload failed")
        completed.add(package.subdir)
        return [package.subdir]

    def upload_repo(repo, *args, **kwargs):
        assert completed == set(mirror.subdirs)
        published.append(repo.subdir)
        return ["repodata"]

    monkeypatch.setattr(Package, "upload", upload_package)
    monkeypatch.setattr(PackageRepo, "upload", upload_repo)
    # Reverse completion order: queue position must not imply a dependency.
    pool_factory = MagicMock()
    pool = pool_factory.return_value.__enter__.return_value
    pool.map.side_effect = lambda func, tasks: [func(t) for t in reversed(tasks)]
    monkeypatch.setattr("conda_oci_mirror.tasks.mp.Pool", pool_factory)

    if fail:
        with pytest.raises(RuntimeError, match="upload failed"):
            mirror.update(serial=serial, dry_run=dry_run)
        assert not published
    else:
        results = mirror.update(serial=serial, dry_run=dry_run)
        assert len(results) == (2 if dry_run else 4)
        assert set(published) == (set() if dry_run else set(mirror.subdirs))


def response(status=200, text='{"packages": {}}'):
    result = requests.Response()
    result.status_code = status
    result._content = text.encode()
    return result


@pytest.mark.parametrize("preload", [False, True])
def test_publish_one_snapshot_with_distinct_layer_filenames(
    tmp_path, monkeypatch, preload
):
    repo = PackageRepo("test", "noarch", tmp_path, "example.com/test")
    get = Mock(side_effect=[response(), response()])
    monkeypatch.setattr("conda_oci_mirror.repo.requests.get", get)
    monkeypatch.setattr(
        Pusher, "push", lambda self, uri: {"uri": uri, "layers": self.layers}
    )
    if preload:
        repo.load_repodata()
    pushed = repo.upload(tmp_path)
    assert get.call_count == 2
    assert all(call.kwargs["timeout"] == 60 for call in get.call_args_list)
    layers = pushed[0]["layers"]
    assert [layer["title"] for layer in layers] == [
        "repodata.json",
        "repodata.json.zst",
    ]

    # Pull both layers through the real filename-selection code.
    client = Registry()
    manifest = {
        "layers": [
            {
                "mediaType": layer["media_type"],
                "digest": "sha256:" + util.sha256sum(layer["path"]),
                "annotations": {"org.opencontainers.image.title": layer["title"]},
            }
            for layer in layers
        ]
    }
    monkeypatch.setattr(client, "get_manifest", Mock(return_value=manifest))
    sources = {
        layer["digest"]: source["path"]
        for layer, source in zip(manifest["layers"], layers)
    }
    monkeypatch.setattr(
        client,
        "download_blob",
        lambda container, digest, outfile: shutil.copyfile(sources[digest], outfile),
    )
    destination = tmp_path / "pulled"
    destination.mkdir()
    client.pull_by_media_type("example.com/test/repodata:latest", str(destination))
    raw = (destination / "repodata.json").read_bytes()
    compressed = (destination / "repodata.json.zst").read_bytes()
    assert json.loads(raw) == {"packages": {}}
    assert zstandard.ZstdDecompressor().decompress(compressed) == raw


@pytest.mark.parametrize("bad_response", [response(500), response(text="invalid json")])
@pytest.mark.parametrize("bad_patches", [False, True])
def test_invalid_metadata_is_not_published(
    tmp_path, monkeypatch, bad_response, bad_patches
):
    repo = PackageRepo("test", "noarch", tmp_path, "example.com/test")
    Path(repo.repodata).write_text('{"packages": {"old": {}}}')
    original = Path(repo.repodata).read_bytes()
    responses = (
        [bad_response, response()] if bad_patches else [response(), bad_response]
    )
    monkeypatch.setattr(
        "conda_oci_mirror.repo.requests.get", Mock(side_effect=responses)
    )
    push = Mock()
    monkeypatch.setattr(Pusher, "push", push)
    with pytest.raises((requests.HTTPError, ValueError)):
        repo.upload(tmp_path)
    push.assert_not_called()
    assert repo.timestamp is None
    assert Path(repo.repodata).read_bytes() == original


def test_missing_optional_metadata_does_not_reuse_stale_file(tmp_path, monkeypatch):
    repo = PackageRepo("test", "noarch", tmp_path, "example.com/test")
    Path(repo.patches).write_text('{"packages": {"old": {}}}')
    monkeypatch.setattr(
        "conda_oci_mirror.repo.requests.get",
        Mock(side_effect=[response(404), response()]),
    )
    assert list(repo.load_repodata().packages) == []
    assert not Path(repo.patches).exists()


def test_tag_cache_is_scoped_and_decodes_consistently(tmp_path, monkeypatch):
    get = Mock(return_value=["1.0__p__local-0"])
    monkeypatch.setattr(oras, "get_tags", get)
    for channel, subdir in [
        ("test", "noarch"),
        ("other", "noarch"),
        ("test", "linux-64"),
    ]:
        repo = PackageRepo(channel, subdir, tmp_path, "example.com/first")
        for registry in ["example.com/first", "example.com/second"]:
            assert repo.get_existing_tags("_demo", registry) == ["1.0+local-0"]
            assert repo.get_existing_tags("_demo", registry) == ["1.0+local-0"]
    assert get.call_count == 6
    assert len({call.args[0] for call in get.call_args_list}) == 6
    assert all(call.args[0].endswith("/zzz_demo") for call in get.call_args_list)


def test_new_scan_refreshes_tags(tmp_path, monkeypatch):
    repo = PackageRepo("test", "noarch", tmp_path, "example.com/test")
    data = RepoData()
    data.data["packages"]["demo-1.0-0.tar.bz2"] = {"name": "demo"}
    monkeypatch.setattr(repo, "load_repodata", lambda *args: data)
    get = Mock(side_effect=[[], ["1.0-0"]])
    monkeypatch.setattr(oras, "get_tags", get)
    assert len(list(repo.find_packages())) == 1
    assert list(repo.find_packages()) == []
    assert get.call_count == 2


def test_manifest_is_refreshed_between_pulls(tmp_path, monkeypatch):
    client = Registry()
    get = Mock(
        side_effect=[
            {"layers": []},
            {
                "layers": [
                    {
                        "mediaType": defaults.repodata_media_type_v1,
                        "digest": "sha256:" + "0" * 64,
                        "annotations": {
                            "org.opencontainers.image.title": "repodata.json"
                        },
                    }
                ]
            },
        ]
    )
    download = Mock(return_value=str(tmp_path / "repodata.json"))
    monkeypatch.setattr(client, "get_manifest", get)
    monkeypatch.setattr(client, "download_blob", download)
    uri = "example.com/test/repodata:latest"
    assert client.pull_by_media_type(uri, str(tmp_path)) == []
    assert client.pull_by_media_type(uri, str(tmp_path)) == [
        str(tmp_path / "repodata.json")
    ]
    assert get.call_count == 2
    download.assert_called_once()


def test_invalid_package_metadata_raises(tmp_path, monkeypatch):
    archive = tmp_path / "demo-1.0-0.conda"
    archive.write_bytes(b"archive")
    package = Package(
        "test",
        "noarch",
        archive.name,
        tmp_path,
        "example.com/test",
        existing_file=str(archive),
    )

    def metadata(self, stage):
        root = Path(stage) / self.package_name
        (root / "info").mkdir(parents=True)
        (root / "info" / "index.json").write_text("{}")
        (root / "info.tar.gz").write_bytes(b"metadata")

    monkeypatch.setattr(Package, "prepare_metadata", metadata)
    push = Mock()
    monkeypatch.setattr(Pusher, "push", push)
    with pytest.raises(ValueError, match="doesn't contain subdir"):
        package.upload()
    push.assert_not_called()
