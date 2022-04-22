import pathlib

import click

from conda_oci_mirror.cache_packages import pull_latest_packages, push_new_packages
from conda_oci_mirror.oci_mirror import mirror as _mirror

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
@click.option("--dry-run/--no-dry-run", default=False, help="Dry run?")
@click.option(
    "--cache-dir", default=pathlib.Path.cwd() / "cache", help="Path to cache directory"
)
def mirror(channel, subdirs, user, packages, host, cache_dir, dry_run):
    cache_dir = pathlib.Path(cache_dir)
    print(f"Using cache dir: {cache_dir}")
    print(f"Mirroring : {channel}")
    print(f"  Subdirs : {subdirs}")
    print(f"  Packages: {packages}")
    print(f"To: {host}/{user}")
    _mirror(
        [channel],
        subdirs,
        packages,
        f"user:{user}",
        host,
        cache_dir=cache_dir,
        dry_run=dry_run,
    )


def push_pull_options(function):
    function = click.option("--location", help="Username for ghcr.io")(function)
    function = click.option("-s", "--subdir")(function)
    function = click.option(
        "-p",
        "--packages",
        help="Select packages for caching",
        default=[],
        multiple=True,
    )(function)
    function = click.option(
        "--host", default="ghcr.io", help="Host to push packages to"
    )(function)
    function = click.option(
        "--cache-dir",
        default=pathlib.Path.cwd() / "cache",
        help="Path to cache directory",
    )(function)
    function = click.option("--dry-run/--no-dry-run", default=False, help="Dry run?")(
        function
    )
    return function


@main.command()
@push_pull_options
def pull_cache(location, subdir, packages, host, cache_dir, dry_run):
    print(subdir)
    pull_latest_packages(f"{host}/{location}", packages, [subdir], cache_dir)


@main.command()
@push_pull_options
def push_cache(location, subdir, packages, host, cache_dir, dry_run):
    push_new_packages(f"{host}/{location}", packages, [subdir], cache_dir)
