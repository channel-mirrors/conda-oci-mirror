import os

import click

import conda_oci_mirror.defaults as defaults
from conda_oci_mirror.logger import setup_logger
from conda_oci_mirror.mirror import Mirror


def warn_skipped(m):
    """Skipped invalid tags are upstream problems, so warn instead of failing."""
    if m.errors:
        click.echo(
            f"Warning: skipped {len(m.errors)} invalid package tags; see warnings above.",
            err=True,
        )

# The cache defaults to the present working directory
default_cache = os.path.join(os.getcwd(), "cache")


@click.group()
def main():
    pass


options = [
    click.option("-s", "--subdir", default=defaults.DEFAULT_SUBDIRS, multiple=True),
    click.option(
        "-p",
        "--package",
        help="Package name or glob pattern (repeatable)",
        default=[],
        multiple=True,
    ),
    click.option(
        "--registry", default=None, help="Registry URI (e.g., ghcr.io/username)"
    ),
    click.option("--dry-run/--no-dry-run", default=False, help="Dry run?"),
    click.option(
        "--workers",
        default=4,
        type=click.IntRange(min=1),
        help="How many workers to use in parallel",
    ),
    click.option(
        "--upload-delay",
        "--timeout",
        "timeout",
        default=500,
        type=click.IntRange(min=0),
        help="Upload delay in milliseconds (not an HTTP timeout)",
    ),
    click.option("--cache-dir", default=default_cache, help="Path to cache directory"),
    click.option("-c", "--channel", help="Select channel", default="conda-forge"),
    click.option("--quiet", is_flag=True, help="Suppress informational logs"),
    click.option("--debug", is_flag=True, help="Enable debug logging"),
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
def mirror(
    channel,
    subdir,
    registry,
    package,
    cache_dir,
    dry_run,
    quiet,
    debug,
    workers,
    timeout,
):
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
        workers=workers,
        timeout=timeout,
    )
    m.update(dry_run)
    warn_skipped(m)


@main.command()
@add_options(options)
def pull_cache(
    channel,
    subdir,
    registry,
    package,
    cache_dir,
    dry_run,
    quiet,
    debug,
    workers,
    timeout,
):
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
        workers=workers,
        timeout=timeout,
    )
    m.pull_latest(dry_run)
    warn_skipped(m)


@main.command()
@add_options(options)
@click.option(
    "--push-all", "--all", is_flag=True, help="Push all selected local packages"
)
def push_cache(
    channel,
    subdir,
    registry,
    package,
    cache_dir,
    dry_run,
    quiet,
    debug,
    push_all,
    workers,
    timeout,
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
        workers=workers,
        timeout=timeout,
    )
    if push_all:
        m.push_all(dry_run)
    else:
        m.push_new(dry_run)
    warn_skipped(m)
