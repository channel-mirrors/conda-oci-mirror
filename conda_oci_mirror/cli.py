import pathlib

import click

import conda_oci_mirror.defaults as defaults
from conda_oci_mirror.cache_packages import pull_latest_packages, push_new_packages
from conda_oci_mirror.mirror import Mirror


@click.group()
def main():
    pass


@main.command()
@click.option("-c", "--channel", help="Select channel")
@click.option("-s", "--subdirs", default=defaults.DEFAULT_SUBDIRS, multiple=True)
@click.option(
    "-p", "--package", help="Select packages for mirroring", default=[], multiple=True
)
@click.option("--user", default=None, help="Username for ghcr.io")
@click.option("--host", default="ghcr.io", help="Host to push packages to")
@click.option("--dry-run/--no-dry-run", default=False, help="Dry run?")
@click.option(
    "--cache-dir", default=pathlib.Path.cwd() / "cache", help="Path to cache directory"
)
@click.option("--quiet", default=False, help="Don't print verbose output?")
def mirror(channel, subdirs, user, package, host, cache_dir, dry_run, quiet):
    m = Mirror(
        channels=[channel],
        subdirs=subdirs,
        packages=package,
        host=host,
        namespace=user,
        cache_dir=cache_dir,
        quiet=quiet,
    )
    m.update(dry_run)


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
    pull_latest_packages(f"{host}/{location}", packages, [subdir], cache_dir)


@main.command()
@push_pull_options
def push_cache(location, subdir, packages, host, cache_dir, dry_run):
    push_new_packages(f"{host}/{location}", packages, [subdir], cache_dir)
