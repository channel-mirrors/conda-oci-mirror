#!/usr/bin/python

import os
import sys
import tarfile

import pytest

# The setup.cfg doesn't install the main module proper
here = os.path.dirname(os.path.abspath(__file__))
root = os.path.dirname(os.path.dirname(here))
sys.path.insert(0, root)
sys.path.insert(0, here)

from helpers import get_mirror  # noqa

from conda_oci_mirror.logger import setup_logger  # noqa

# import conda_oci_mirror.defaults as defaults
from conda_oci_mirror.repo import PackageRepo  # noqa

# Ensure we see all verbosity
setup_logger(debug=True, quiet=False)


# Linux 64 for zlib has a lot of versions
@pytest.mark.parametrize(
    "subdir,pkg,ext",
    [
        ("linux-64", "zlib", "tar.bz2"),
        ("noarch", "zope.event", "conda"),
    ],
)
def test_package_repo(tmp_path, subdir, pkg, ext):
    """
    Test package repo

    A package repository is a wrapper around a subdirectory.
    Optionally it can have a registry and then we can interact
    with a remote. We use zlib with linux-64 since it has a lot
    of versions.
    """
    cache_dir = os.path.join(tmp_path, "cache")

    # Do a quick mirror so we have the package to get in a remote!
    m = get_mirror(subdir, cache_dir, package=pkg)

    # There is no latest tag, so we need to get tags from here
    res = m.update()

    # Get a package URI (last one should be latest)
    # Note that if you run this test twice on the same registry
    # since the packages are already mirrored you'll get an empty list
    package = [x for x in res if pkg in x["uri"]][-1]
    assert package

    # 'zlib:1.2.11-0' or zope..etc
    package_name = package["uri"].rsplit("/", 1)[-1]

    # Our package remote is "dinosaur" and not "conda-forge"
    repo = PackageRepo(m.channel, subdir, cache_dir, registry=m.registry)

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
    assert pkg.endswith(ext)
    assert os.path.exists(pkg)
