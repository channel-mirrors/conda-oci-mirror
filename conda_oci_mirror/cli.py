import os

import click

import conda_oci_mirror.defaults as defaults
from conda_oci_mirror.mirror import Mirror

# The cache defaults to the present working directory
default_cache = os.path.join(os.getcwd(), "cache")


@click.group()
def main():
    pass


options = [
    click.option("-s", "--subdir", default=defaults.DEFAULT_SUBDIRS, multiple=True),
    click.option("-p", "--package", help="Select packages", default=[], multiple=True),
    click.option("--user", default=None, help="Username for ghcr.io"),
    click.option("--host", default="ghcr.io", help="Host to push packages to"),
    click.option("--dry-run/--no-dry-run", default=False, help="Dry run?"),
    click.option("--cache-dir", default=default_cache, help="Path to cache directory"),
    click.option("-c", "--channel", help="Select channel", default="conda-forge"),
]


def add_options(options):
    """
    Function to return click options (all shared between commands)
    """

    def _add_options(func):
        for option in reversed(options):
            func = option(func)
        return func

    return _add_options


@main.command()
@add_options(options)
def mirror(channel, subdir, user, package, host, cache_dir, dry_run):
    m = Mirror(
        channels=[channel],
        subdirs=subdir,
        packages=package,
        host=host,
        namespace=user,
        cache_dir=cache_dir,
    )
    m.update(dry_run)


@main.command()
@add_options(options)
def pull_cache(channel, user, subdir, package, host, cache_dir, dry_run):
    """
    Pull a remote host/user to a local cache_dir
    """
    m = Mirror(
        channels=[channel],
        subdirs=subdir,
        packages=package,
        host=host,
        namespace=user,
        cache_dir=cache_dir,
    )
    m.pull_latest(dry_run)


@main.command()
@add_options(options)
def push_cache(channel, user, subdir, package, host, cache_dir, dry_run):
    """
    Push a local cache in cache_dir to a remote host/user
    """
    m = Mirror(
        channels=[channel],
        subdirs=subdir,
        packages=package,
        host=host,
        namespace=user,
        cache_dir=cache_dir,
    )
    m.push_new(dry_run)
