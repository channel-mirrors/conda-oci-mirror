import json

from conda_package_handling import api

from conda_oci_mirror import defaults
from conda_oci_mirror.mirror import Mirror
from conda_oci_mirror.repo import PackageRepo


def test_both_formats_roundtrip_with_parallel_workers(tmp_path, oci_registry):
    prefix = tmp_path / "prefix"
    (prefix / "info").mkdir(parents=True)
    (prefix / "info" / "index.json").write_text(
        json.dumps(
            {
                "name": "demo",
                "version": "1",
                "build": "0",
                "build_number": 0,
                "subdir": "noarch",
                "depends": [],
            }
        )
    )
    (prefix / "info" / "files").write_text("payload.txt\n")
    (prefix / "payload.txt").write_text("payload\n")
    cache = tmp_path / "cache" / "test" / "noarch"
    cache.mkdir(parents=True)
    expected = {}
    for ext in ("conda", "tar.bz2"):
        name = f"demo-1-0.{ext}"
        api.create(
            str(prefix),
            ["info/index.json", "info/files", "payload.txt"],
            name,
            out_folder=str(cache),
        )
        expected[name] = (cache / name).read_bytes()

    mirror = Mirror(
        "test",
        ["demo"],
        subdirs=["noarch"],
        registry=oci_registry,
        cache_dir=tmp_path / "cache",
        timeout=0,
        workers=2,
    )
    assert len(mirror.push_all()) == 2
    assert {name: (cache / name).read_bytes() for name in expected} == expected
    repo = PackageRepo(
        "test", "noarch", tmp_path / "pulls", mirror.registry, client=mirror.client
    )
    for ext in ("conda", "tar.bz2"):
        assert repo.get_existing_packages("demo", package_ext=ext) == {
            f"demo-1-0.{ext}"
        }
    assert repo.get_package("demo:1-0").endswith(".conda")
    assert repo.get_index_json("demo:1-0")["name"] == "demo"
    uri = f"{mirror.registry}/test/noarch/demo:1-0"
    for media_type in (
        defaults.package_conda_media_type,
        defaults.package_tarbz2_media_type,
    ):
        paths = mirror.client.pull_by_media_type(
            uri, str(tmp_path / "pulls"), media_type
        )
        assert len(paths) == 1
    for name, content in expected.items():
        assert (tmp_path / "pulls" / name).read_bytes() == content

    # A later push with only .conda locally must retain the remote tar.bz2 layer.
    (cache / "demo-1-0.tar.bz2").unlink()
    assert len(mirror.push_all()) == 1
    layers = mirror.client.get_manifest(uri)["layers"]
    assert {layer["mediaType"] for layer in layers} >= {
        defaults.package_conda_media_type,
        defaults.package_tarbz2_media_type,
    }
