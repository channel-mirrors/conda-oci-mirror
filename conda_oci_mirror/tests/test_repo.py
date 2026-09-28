import hashlib
import json
import os
import sys
import tarfile
from pathlib import Path

import msgpack
import pytest
import zstandard as zstd

import conda_oci_mirror.repo as repository
from conda_oci_mirror import defaults
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


def test_upload_pushes_only_shards_missing_from_the_published_index(
    tmp_path, monkeypatch
):
    shards = {name: name.encode() for name in ("old", "new", "extra")}
    digests = {name: hashlib.sha256(data).hexdigest() for name, data in shards.items()}

    def pack_index(names):
        return zstd.ZstdCompressor().compress(
            msgpack.packb(
                {
                    "info": {"base_url": "", "shards_base_url": ""},
                    "shards": {name: bytes.fromhex(digests[name]) for name in names},
                }
            )
        )

    upstream = {"index": pack_index(["old"])}
    downloads = []

    class Response:
        def __init__(self, content, status_code=200):
            self.content = content
            self.status_code = status_code
            self.text = content.decode() if content.startswith(b"{") else ""

        def raise_for_status(self):
            if self.status_code != 200:
                raise AssertionError(self.status_code)

        def json(self):
            return json.loads(self.text)

    def get(url, **_):
        if url.endswith("repodata.json"):
            return Response(json.dumps({"packages": {}, "packages.conda": {}}).encode())
        if url.endswith("repodata_from_packages.json"):
            return Response(b"{}")
        if url.endswith("repodata_shards.msgpack.zst"):
            return Response(upstream["index"])
        for name, digest in digests.items():
            if url == f"https://conda.anaconda.org/test/linux-64/{digest}.msgpack.zst":
                downloads.append(name)
                return Response(shards[name])
        raise AssertionError(f"unexpected URL: {url}")

    class Client:
        """Stands in for the registry; remembers the latest published manifest."""

        def __init__(self):
            self.latest = None

        def get_optional_manifest(self, uri):
            assert uri == "ghcr.io/example/test/linux-64/repodata.json:latest"
            return self.latest

        def download_blob(self, uri, digest, outfile):
            layer = next(
                layer for layer in self.latest["layers"] if layer["digest"] == digest
            )
            Path(outfile).write_bytes(Path(layer["path"]).read_bytes())
            return outfile

        def get_tags(self, uri, N=None):
            assert uri == "ghcr.io/example/test/linux-64/repodata.json/shards"
            listings.append(uri)
            return sorted(registry_tags)

        def push_single_layer(self, uri, path, media_type, title):
            # Shards are pushed from per-thread copies of this client.
            assert self is not client
            digest = uri.rsplit(":", 1)[1]
            assert hashlib.sha256(Path(path).read_bytes()).hexdigest() == digest
            registry_tags.add(digest)

    registry_tags = set()
    listings = []
    client = Client()

    class Pusher:
        def __init__(self, root, timestamp, client=None):
            self.layers = []
            self.created_at = "2026.01.02.03.04"

        def add_layer(self, path, media_type, title=None):
            # Snapshot the content: the cached index is rewritten on the next run.
            snapshot = (
                tmp_path / "published" / hashlib.sha256(path.encode()).hexdigest()
            )
            snapshot.parent.mkdir(exist_ok=True)
            snapshot.write_bytes(Path(path).read_bytes())
            digest = f"sha256:{hashlib.sha256(snapshot.read_bytes()).hexdigest()}"
            self.layers.append(
                {
                    "path": str(snapshot),
                    "mediaType": media_type,
                    "digest": digest,
                    "title": title,
                }
            )

        def push(self, uri):
            if uri.endswith(":latest"):
                client.latest = {"layers": self.layers}
            return {"uri": uri, "layers": self.layers}

    monkeypatch.setattr(repository.requests, "get", get)
    monkeypatch.setattr(repository, "Pusher", Pusher)

    def upload():
        repo = PackageRepo(
            "test", "linux-64", tmp_path, registry="ghcr.io/example", client=client
        )
        return repo, repo.upload(tmp_path)

    # First run: nothing is published yet, so every shard is pushed first.
    repo, pushes = upload()
    shard_uri = "ghcr.io/example/test/linux-64/repodata.json/shards"
    assert [push["uri"] for push in pushes[:-2]] == [f"{shard_uri}:{digests['old']}"]
    assert pushes[0]["layers"][0]["media_type"] == defaults.repodata_shard_media_type_v1
    assert pushes[-1]["uri"] == "ghcr.io/example/test/linux-64/repodata.json:latest"
    assert defaults.repodata_shards_media_type_v1 in {
        layer["mediaType"] for layer in pushes[-1]["layers"]
    }
    mirrored_index = msgpack.unpackb(
        zstd.ZstdDecompressor().decompress(Path(repo.shard_index).read_bytes()),
        raw=False,
    )
    assert mirrored_index["info"]["base_url"] == ""
    assert mirrored_index["info"]["shards_base_url"] == "./shards/"

    # Second run with a fresh cache: only the shard missing from the published
    # index is downloaded and pushed.
    for path in Path(repo.shards_dir).iterdir():
        path.unlink()
    downloads.clear()
    upstream["index"] = pack_index(["old", "new"])
    listings.clear()
    _, pushes = upload()
    assert downloads == ["new"]
    assert [push["uri"] for push in pushes[:-2]] == [f"{shard_uri}:{digests['new']}"]
    # With an index published, the shard tags are never listed.
    assert listings == []

    # A first push that was cut off before any index was published resumes:
    # one tag listing, and only the shards missing from it are pushed.
    client.latest = None
    upstream["index"] = pack_index(["old", "new", "extra"])
    downloads.clear()
    _, pushes = upload()
    assert listings == [shard_uri]
    assert downloads == ["extra"]
    assert [push["uri"] for push in pushes[:-2]] == [f"{shard_uri}:{digests['extra']}"]
    assert registry_tags == set(digests.values())

    # An index published with the legacy "+zstd" media type still counts, so the
    # first run after the switch to "+zst" does not list the shard tags.
    for layer in client.latest["layers"]:
        if layer["mediaType"] == defaults.repodata_shards_media_type_v1:
            layer["mediaType"] = defaults.legacy_repodata_shards_media_type_v1
    listings.clear()
    downloads.clear()
    _, pushes = upload()
    assert listings == []
    assert downloads == []
    assert defaults.repodata_shards_media_type_v1 in {
        layer["mediaType"] for layer in pushes[-1]["layers"]
    }


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
            m.channel,
            subdir=subdir,
            cache_dir=m.cache_dir,
            registry=m.registry,
            client=m.client,
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
            should_find.discard(member.name)

        if should_find:
            raise ValueError(f"Expected to find {should_find} in info, but did not.")

        # Get package will look first for the conda media type, then old format bz2
        pkg = repo.get_package(package_name)

        # Find the layer with the media type
        layer = next(x for x in result["layers"] if "conda.package" in x["media_type"])
        assert os.path.basename(layer["path"]) == os.path.basename(pkg)
