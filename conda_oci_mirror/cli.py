import os

import click

import conda_oci_mirror.defaults as defaults
from conda_oci_mirror.logger import setup_logger
from conda_oci_mirror.mirror import Mirror

# The cache defaults to the present working directory
default_cache = os.path.join(os.getcwd(), "cache")


@click.group()
def main():
    pass


options = [
    click.option("-s", "--subdir", default=defaults.DEFAULT_SUBDIRS, multiple=True),
    click.option("-p", "--package", help="Select packages", default=[], multiple=True),
    click.option(
        "--registry", default=None, help="Registry URI (e.g., ghcr.io/username)"
    ),
    click.option("--dry-run/--no-dry-run", default=False, help="Dry run?"),
    click.option("--cache-dir", default=default_cache, help="Path to cache directory"),
    click.option("-c", "--channel", help="Select channel", default="conda-forge"),
    click.option("--quiet", default=False, help="Do not print verbose output?"),
    click.option("--debug", default=False, help="Print debug output?"),
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
def mirror(channel, subdir, registry, package, cache_dir, dry_run, quiet, debug):
    setup_logger(
        quiet=quiet,
        debug=debug,
    )
    m = Mirror(
        channel=channel,
        subdirs=subdir,
        packages=package,
        registry=registry,
        cache_dir=cache_dir,
    )
    m.update(dry_run)


@main.command()
@add_options(options)
def pull_cache(channel, subdir, registry, package, cache_dir, dry_run, quiet, debug):
    """
    Pull a remote host/user to a local cache_dir
    """
    setup_logger(
        quiet=quiet,
        debug=debug,
    )
    m = Mirror(
        channel=channel,
        subdirs=subdir,
        packages=package,
        registry=registry,
        cache_dir=cache_dir,
    )
    m.pull_latest(dry_run)


@main.command()
@add_options(options)
@click.option("--push-all", default=False, help="Push all local packages?")
def push_cache(
    channel, subdir, registry, package, cache_dir, dry_run, quiet, debug, push_all
):
    """
    Push a local cache in cache_dir to a remote host/user
    """
    setup_logger(
        quiet=quiet,
        debug=debug,
    )
    m = Mirror(
        channel=channel,
        subdirs=subdir,
        packages=package,
        registry=registry,
        cache_dir=cache_dir,
    )
    if push_all:
        m.push_all(dry_run)
    else:
        m.push_new(dry_run)
