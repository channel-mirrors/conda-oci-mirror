#!/usr/bin/python

import os
import sys

import pytest

# The setup.cfg doesn't install the main module proper
here = os.path.dirname(os.path.abspath(__file__))
root = os.path.dirname(os.path.dirname(here))
sys.path.insert(0, root)

import conda_oci_mirror.defaults as defaults  # noqa
import conda_oci_mirror.util as util  # noqa
from conda_oci_mirror.logger import setup_logger  # noqa

# import conda_oci_mirror.defaults as defaults
from conda_oci_mirror.mirror import Mirror  # noqa
from conda_oci_mirror.oras import oras  # noqa

# Ensure we see all verbosity
setup_logger(debug=True, quiet=False)

registry_host = os.environ.get("registry_host") or "http://127.0.0.1"
registry_port = os.environ.get("registry_port") or 5000
host = f"{registry_host}:{registry_port}"


def get_tags(uri):
    """
    Helper function to get tags
    """
    result = oras.get_tags(uri)
    assert result.status_code == 200
    result = result.json()
    assert "tags" in result
    return result


@pytest.mark.parametrize(
    "subdir",
    defaults.DEFAULT_SUBDIRS,
)
def test_mirror(tmp_path, subdir):
    """
    Test creation of a mirror

    This test does a basic sanity check that when we run mirror,
    we are pushing to the registry the repodata and (if it exists)
    the package files. We verify by pull back again with oras,
    and checking file structure and/or size.
    """
    cache_dir = os.path.join(tmp_path, "cache")
    channel = "conda-forge"
    package = "redo"
    user = "dinosaur"

    m = Mirror(
        channels=[channel],
        packages=[package],
        host=host,
        subdirs=[subdir],
        namespace=user,
        cache_dir=cache_dir,
    )
    assert len(m.update()) >= 2

    # Each subdir should have a directory in the cache with repodata
    # and nothing else
    cache_subdir = os.path.join(cache_dir, channel, subdir)
    assert os.path.exists(cache_subdir)
    "repodata.json" in os.listdir(cache_subdir)
    len(os.listdir(cache_subdir)) == 1
    repodata_file = os.path.join(cache_subdir, "repodata.json")
    repodata = util.read_json(repodata_file)

    # Smallest size is osx-arm64
    assert "packages" in repodata and len(repodata["packages"]) >= 40000

    # We can use oras to get artifacts we should have pushed
    # We should be able to pull the latest tag
    expected_latest = f"{host}/dinosaur/{channel}/{subdir}/repodata.json:latest"
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
    package_names = set([x["name"] for _, x in repodata["packages"].items()])

    # Looks like only noarch has redo
    if subdir != "noarch":
        assert package not in package_names
        return
    assert package in package_names

    expected_repo = f"http://127.0.0.1:5000/{user}/{channel}/{subdir}/{package}"
    tags = get_tags(expected_repo)
    assert "latest" in tags["tags"]
    assert len(tags["tags"]) >= 2

    pull_dir = os.path.join(tmp_path, "package")
    result = oras.pull(target=f"{expected_repo}:latest", outdir=pull_dir)
    assert result

    # This directory has .bz2 and subdirectory
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
            continue

        assert fullpath.endswith("bz2")
