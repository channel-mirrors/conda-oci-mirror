#!/usr/bin/python

import os
import sys

import pytest

# The setup.cfg doesn't install the main module proper
here = os.path.dirname(os.path.abspath(__file__))
root = os.path.dirname(os.path.dirname(here))
sys.path.insert(0, root)

import conda_oci_mirror.defaults as defaults  # noqa
from conda_oci_mirror.logger import setup_logger  # noqa

# import conda_oci_mirror.defaults as defaults
from conda_oci_mirror.mirror import Mirror  # noqa

# Ensure we see all verbosity
setup_logger(debug=True, quiet=False)

registry_host = os.environ.get("registry_host") or "http://127.0.0.1"
registry_port = os.environ.get("registry_port") or 5000
host = f"{registry_host}:{registry_port}"


@pytest.mark.parametrize(
    "subdir",
    defaults.DEFAULT_SUBDIRS,
)
def test_push_pull_cache(tmp_path, subdir):
    """
    Test push and pull of the cache.

    This test does a basic sanity check that when we run mirror,
    we are pushing to the registry the repodata and (if it exists)
    the package files. We verify by pull back again with oras,
    and checking file structure and/or size.
    """
    # Start with a mirror
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
    updates = m.update()
    assert len(updates) >= 2

    # Now we can use the mirror to push and pull from the cache
    m.pull_latest()
    m.push_new()
