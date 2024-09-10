#!/usr/bin/python

import os
import sys

import pytest

from conda_oci_mirror.logger import setup_logger

# The setup.cfg doesn't install the main module proper
here = os.path.dirname(os.path.abspath(__file__))
root = os.path.dirname(os.path.dirname(here))
sys.path.insert(0, root)
sys.path.insert(0, here)

# Ensure we see all verbosity
setup_logger(debug=True, quiet=False)


# override the subdir that is used in the tests
@pytest.fixture
def subdir():
    return "noarch"


def test_push_pull_cache(mirror_instance):
    """
    Test push and pull of the cache.

    This test does a basic sanity check that when we run mirror,
    we are pushing to the registry the repodata and (if it exists)
    the package files. We verify by pull back again with oras,
    and checking file structure and/or size.
    """

    # Start with a mirror
    m = mirror_instance
    assert m.subdirs == ["noarch"]

    updates = m.update(serial=True)
    assert len(updates) >= 6

    # Now we can use the mirror to push and pull from the cache
    latest = m.pull_latest(serial=True)
    assert len(latest) >= 2

    # Should not be any new if we just mirrored
    new_packages = m.push_new(serial=True)
    assert not new_packages
    all_packages = m.push_all(serial=True)
    assert len(all_packages) >= 2

    # Another update should only concern repodata
    updates = m.update(serial=True)
    assert len(updates) == 2
