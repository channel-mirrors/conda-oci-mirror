#!/usr/bin/python

import os
import sys

import pytest

# The setup.cfg doesn't install the main module proper
here = os.path.dirname(os.path.abspath(__file__))
root = os.path.dirname(os.path.dirname(here))
sys.path.insert(0, root)

from helpers import check_media_type, get_mirror, get_tags  # noqa

import conda_oci_mirror.repo as repository  # noqa
from conda_oci_mirror.logger import setup_logger  # noqa

# import conda_oci_mirror.defaults as defaults
from conda_oci_mirror.oras import oras  # noqa

# Ensure we see all verbosity
setup_logger(debug=True, quiet=False)


@pytest.mark.parametrize(
    "subdir,num_updates,package_name",
    [
        ("linux-64", 5, "xtensor"),
        ("osx-64", 6, "xtensor"),
        ("osx-arm64", 5, "xtensor"),
        ("win-64", 2, "xtensor"),
        ("linux-aarch64", 2, "xtensor"),
        ("linux-ppc64le", 2, "xtensor"),
        ("noarch", 6, "redo"),
    ],
)
def test_mirror(tmp_path, subdir, num_updates, package_name):
    """
    Test creation of a mirror

    This test does a basic sanity check that when we run mirror,
    we are pushing to the registry the repodata and (if it exists)
    the package files. We verify by pull back again with oras,
    and checking file structure and/or size.
    """
    cache_dir = os.path.join(tmp_path, "cache")
    m = get_mirror(cache_dir, subdir=subdir)
    cache_subdir = os.path.join(cache_dir, m.channel, m.subdirs[0])

    assert not os.path.exists(cache_subdir)
    updates = m.update(serial=True)
    print(subdir)
    print(len(updates))
    assert len(updates) == num_updates

    # Sanity check structure of layers
    for update in updates:
        for key in "uri", "layers":
            assert key in update
        for layer in update["layers"]:
            for key in ["path", "title", "media_type", "annotations"]:
                assert key in layer
            # The title should be the file basename
            assert layer["path"].endswith(layer["title"])

            check_media_type(layer)

            # The annotation is needed for the path
            assert "org.opencontainers.image.title" in layer["annotations"]
            assert (
                layer["annotations"]["org.opencontainers.image.title"] == layer["title"]
            )

    # Each subdir should have a directory in the cache with repodata
    # and nothing else
    cache_subdir = os.path.join(cache_dir, m.channel, subdir)
    assert os.path.exists(cache_subdir)
    "repodata.json" in os.listdir(cache_subdir)
    len(os.listdir(cache_subdir)) == 1
    repodata_file = os.path.join(cache_subdir, "repodata.json")
    repodata = repository.RepoData(repodata_file)

    # The repodata is len(packages) + 2 (latest and tag for repodata.json)
    assert len(list(repodata.packages)) + 2 == num_updates

    # Smallest size is osx-arm64
    for key in ["packages", "packages.conda"]:
        assert key in repodata.data

    # We can use oras to get artifacts we should have pushed
    # We should be able to pull the latest tag
    expected_latest = f"{m.registry}/{m.channel}/{subdir}/repodata.json:latest"
    result = get_tags(expected_latest)

    # We minimally should have 2, one which is latest
    assert "latest" in result["tags"]
    assert len(result["tags"]) >= 2

    pull_dir = os.path.join(tmp_path, "pulls")
    result = oras.pull(target=expected_latest, outdir=pull_dir)
    assert result
    assert os.path.exists(result[0])

    # It should be the same file!
    os.stat(result[0]).st_size == os.stat(repodata_file).st_size
    package_names = repodata.package_names

    # Testing mirror has xtensor, except if only 2
    if num_updates == 2:
        assert not package_names
        return

    print(package_names)
    assert package_name in package_names

    expected_repo = f"{m.registry}/{m.channel}/{subdir}/{package_name}"
    tags = get_tags(expected_repo)
    assert len(tags["tags"]) >= 1

    # Get the latest tag - should be newer at end (e.g., conda)
    tag = tags["tags"][-1]
    pull_dir = os.path.join(tmp_path, "package")
    uri = f"{expected_repo}:{tag}"
    result = oras.pull(target=uri, outdir=pull_dir)
    assert result

    # This directory has .bz2 or conda and subdirectory
    # Here we are checking for the package archive and info files
    found = os.listdir(pull_dir)
    assert len(found) == 2
    for result in found:
        fullpath = os.path.join(pull_dir, result)
        if os.path.isdir(fullpath):
            assert "info" in os.listdir(fullpath)
            assert "info.tar.gz" in os.listdir(fullpath)
            fullpath = os.path.join(pull_dir, result, "info")
            assert "index.json" in os.listdir(fullpath)
