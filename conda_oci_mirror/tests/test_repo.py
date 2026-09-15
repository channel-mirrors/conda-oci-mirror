#!/usr/bin/python

import hashlib
import json
import os
import sys
import tarfile
from pathlib import Path

import msgpack
import pytest
import zstandard as zstd

import conda_oci_mirror.defaults as defaults
import conda_oci_mirror.repo as repository
from conda_oci_mirror.logger import setup_logger
from conda_oci_mirror.repo import PackageRepo, RepoData

# Ensure we see all verbosity
setup_logger(debug=True, quiet=False)


# The setup.cfg doesn't install the main module proper
here = os.path.dirname(os.path.abspath(__file__))
root = os.path.dirname(os.path.dirname(here))
sys.path.insert(0, root)
sys.path.insert(0, here)


class TestRepoData:
    @pytest.fixture
    def repo_data(self) -> RepoData:
        test_repodata_file = Path(__file__).parent / "test_repodata.json"
        repodata = RepoData(test_repodata_file)
        return repodata

    def test_get_latest_tag(self, repo_data):
        assert repo_data.get_latest_tag("pytest") == "7.2.0-py310hbbe02a8_1"


def test_upload_publishes_shards_before_the_index(tmp_path, monkeypatch):
    shard = b"shard"
    digest = hashlib.sha256(shard).hexdigest()
    shard_index = zstd.ZstdCompressor().compress(
        msgpack.packb(
            {
                "info": {"base_url": "../packages/", "shards_base_url": "./shards/"},
                "shards": {"demo": bytes.fromhex(digest)},
            }
        )
    )

    class Response:
        def __init__(self, content, status_code=200):
            self.content = content
            self.status_code = status_code
            self.text = content.decode() if content.startswith(b"{") else ""

    def get(url, **_):
        if url.endswith("repodata.json"):
            return Response(json.dumps({"packages": {}, "packages.conda": {}}).encode())
        if url.endswith("repodata_from_packages.json"):
            return Response(b"{}")
        if url.endswith("repodata_shards.msgpack.zst"):
            return Response(shard_index)
        if url.endswith(f"shards/{digest}.msgpack.zst"):
            return Response(shard)
        raise AssertionError(f"unexpected URL: {url}")

    class Pusher:
        def __init__(self, root, timestamp):
            self.layers = []
            self.created_at = "2026.01.02.03.04"

        def add_layer(self, path, media_type, title=None):
            self.layers.append({"path": path, "media_type": media_type, "title": title})

        def push(self, uri):
            return {"uri": uri, "layers": self.layers}

    monkeypatch.setattr(repository.requests, "get", get)
    monkeypatch.setattr(repository, "Pusher", Pusher)

    repo = PackageRepo("test", "linux-64", tmp_path, registry="ghcr.io/example")
    monkeypatch.setattr(repo, "get_existing_tags", lambda *_: [])
    pushes = repo.upload(tmp_path)

    assert pushes[0]["uri"] == f"ghcr.io/example/test/linux-64/shards:{digest}"
    assert pushes[0]["layers"][0]["media_type"] == defaults.repodata_shard_media_type_v1
    assert pushes[-1]["uri"] == "ghcr.io/example/test/linux-64/repodata.json:latest"
    assert defaults.repodata_shards_media_type_v1 in {
        layer["media_type"] for layer in pushes[-1]["layers"]
    }
    mirrored_index = msgpack.unpackb(
        zstd.ZstdDecompressor().decompress(Path(repo.shard_index).read_bytes()),
        raw=False,
    )
    assert mirrored_index["info"]["base_url"] == ""
    assert mirrored_index["info"]["shards_base_url"] == "./shards/"


def test_package_repo(mirror_instance):
    """
    Test package repo

    A package repository is a wrapper around a subdirectory.
    Optionally it can have a registry and then we can interact
    with a remote. We use zlib with linux-64 since it has a lot
    of versions.
    """
    # Do a quick mirror so we have the package to get in a remote!
    m = mirror_instance

    # There is no latest tag, so we need to get tags from here
    res = m.update(serial=True)
    # TODO ask @vsoch if this is wrong now
    assert len(res) >= 20

    # Get a package URI (last one should be latest)
    # Note that if you run this test twice on the same registry
    # since the packages are already mirrored you'll get an empty list
    for result in res:
        if "repodata" in result["uri"]:
            continue
        print(result["uri"])
        package_name = result["uri"].rsplit("/", 1)[-1]
        subdir = result["uri"].split("/")[-2]

        # Our package remote is "dinosaur" and not "conda-forge"
        repo = PackageRepo(
            m.channel, subdir=subdir, cache_dir=m.cache_dir, registry=m.registry
        )

        # Should retrieve from
        # http://127.0.0.1:5000/dinosaur/conda-forge/linux-64/zlib:1.2.11-0'
        index_json = repo.get_index_json(package_name)

        # Assert this is an index json!
        for required in [
            "arch",
            "build",
            "build_number",
            "depends",
            "license",
            "name",
            "platform",
            "subdir",
            "version",
        ]:
            assert required in index_json

        # Now get the info, this is an opened tarfile
        info = repo.get_info(package_name)
        assert isinstance(info, tarfile.TarFile)
        members = list(info)

        # These are the names we expect to see (shared between formats)
        should_find = {
            "files",
            "index.json",
            "recipe",
        }
        for member in members:
            print(f"Found zlib info member {member.name}")
            if member.name in should_find:
                should_find.remove(member.name)

        if should_find:
            raise ValueError(f"Expected to find {should_find} in info, but did not.")

        # Get package will look first for the conda media type, then old format bz2
        pkg = repo.get_package(package_name)

        # Find the layer with the media type
        layer = [x for x in result["layers"] if "conda.package" in x["media_type"]][0]
        assert os.path.basename(layer["path"]) == os.path.basename(pkg)
