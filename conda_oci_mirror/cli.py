
import pathlib
import click

from conda_oci_mirror.oci_mirror import mirror as _mirror
from conda_oci_mirror.functions import compare_checksums as _compare_checksums
DEFAULT_SUBDIRS = [
    "linux-64",
    "osx-64",
    "osx-arm64",
    "win-64",
    "linux-aarch64",
    "linux-ppc64le",
    "noarch",
]


@click.group()
def main():
    pass


@main.command()
@click.option("-c", "--channel", help="Select channel")
@click.option("-s", "--subdirs", default=DEFAULT_SUBDIRS, multiple=True)
@click.option(
    "-p", "--packages", help="Select packages for mirroring", default=[], multiple=True
)
@click.option("--user", default=None, help="Username for ghcr.io")
@click.option("--host", default="ghcr.io", help="Host to push packages to")
@click.option(
    "--cache-dir", default=pathlib.Path.cwd() / "cache", help="Path to cache directory"
)
def mirror(channel, subdirs, user, packages, host, cache_dir):
    cache_dir = pathlib.Path(cache_dir)
    print(f"Using cache dir: {cache_dir}")
    print(f"Mirroring : {channel}")

    print(f"  Subdirs : {subdirs}")
    print(f"  Packages: {packages}")
    #####
    print(f"To: {host}/{user}")
    _mirror([channel], subdirs, packages, f"user:{user}", host, cache_dir=cache_dir)

@main.command()
@click.option("-b", "--base", help="Select the the parent directory where all subdir are")
@click.option("-s", "--subdirs", default=DEFAULT_SUBDIRS, multiple=True)
def check(base, subdirs):
#base: /home/runner/work/mirror_conda/mirror_conda/cache/conda-forge/
    cache_dir = pathlib.Path(cache_dir)
    print(f"Using cache dir: {cache_dir}")
    result = []
    result = _compare_checksums(base,subdirs)
    if len(result) == 0:
        print ("No inconsistencies found while comparing the checksums")
    else:
        print ("Inconsistencies found in while comparing the checksums")
        print (result)

