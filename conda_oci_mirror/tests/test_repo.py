#!/usr/bin/python

import os
import sys
import tarfile
from pathlib import Path

import pytest

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
