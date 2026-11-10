import datetime
import os
import pathlib
import tempfile

import requests

import conda_oci_mirror.package as pkg
import conda_oci_mirror.repo as repository
from conda_oci_mirror import decorators, defaults, tasks, util
from conda_oci_mirror.logger import logger
from conda_oci_mirror.oras import get_oras_client, registry_name


def get_forbidden_packages():
    """
    Get listing of undistributable packages from conda.
    """
    response = requests.get(defaults.forbidden_package_url)
    if response.status_code != 200:
        raise ValueError(
            f"Cannot retrieve forbidden packages from {defaults.forbidden_package_url}"
        )
    return response.json()["undistributable"]


class Mirror:
    """
    A Mirror represents a conda Mirror with an associated registry.
    """

    def __init__(
        self,
        channel,
        packages,
        subdirs=None,
        registry=None,
        cache_dir=None,
        quiet=False,
        insecure=False,
        workers=4,
        timeout=700,
        shard_workers=2,
        repush_shards=False,
        use_cache=True,
    ):
        self.channel = channel
        self.subdirs = subdirs or defaults.DEFAULT_SUBDIRS
        self.packages = [packages] if isinstance(packages, str) else (packages or [])
        if "all" in self.packages:
            self.packages = []
        # TODO consider placing packages on level of functions
        # We should not need to specify them on init.

        # Default mirrors are here
        registry = registry or "ghcr.io/channel-mirrors"
        self.client = get_oras_client(registry, insecure=insecure)
        self.registry = registry_name(registry)

        # Not every command reads or writes the cache; those do not announce one.
        self.cache_dir = (
            os.path.abspath(cache_dir or defaults.CACHE_DIR) if use_cache else None
        )
        self.quiet = quiet
        self.announce()

        # Set listing of (undistributable) packages to skip
        self.skip_packages = (
            get_forbidden_packages() if channel == "conda-forge" else None
        )

        # Set the number of workers
        self.workers = workers
        self.shard_workers = shard_workers
        self.repush_shards = repush_shards

        # Set the timeout
        self.timeout = timeout / 1000.0
        self.errors = []

    def announce(self):
        """
        Show metadata about the mirror setup
        """
        if self.cache_dir:
            util.print_item("Using cache dir:", self.cache_dir)
        util.print_item(" Channel  :", self.channel)
        util.print_item("  Subdirs :", self.subdirs)
        util.print_item("  Packages:", "all" if not self.packages else self.packages)

    @decorators.require_registry
    def update(self, dry_run=False, serial=False, include_yanked=True):
        """
        Update from a conda mirror (do a mirror) akin to a pull and a push.
        """
        self.errors.clear()
        util.print_item("To: ", self.registry)

        # Publish metadata only after every package upload has succeeded.
        runner = tasks.TaskRunner(workers=self.workers)
        repo_runner = tasks.TaskRunner(workers=self.workers)

        # If they think they are pushing but no auth, they are not :)
        if not self.client.has_auth and dry_run is False:
            logger.warning(
                "ORAS is not authenticated, if you registry requires auth this will not work"
            )

        for subdir, cache_dir in self.iter_subdirs():
            repo = repository.PackageRepo(
                self.channel,
                subdir,
                cache_dir,
                self.registry,
                client=self.client,
                mirror_shards=not self.packages,
                shard_workers=self.shard_workers,
                repush_shards=self.repush_shards,
            )

            # Run filter based on packages we are looking for, and forbidden
            # This includes packages and packages.conda. If include yanked is true,
            # this means we use repodata_from_packages.json that includes removed.
            for package, info in repo.find_packages(
                self.packages, self.skip_packages, include_yanked=include_yanked
            ):
                # Add the new tasks to be run by the runner
                # This will get mapped into a Package instance to interact with
                task = pkg.Package(
                    self.channel,
                    subdir,
                    package,
                    cache_dir,
                    self.registry,
                    info=info,
                    client=self.client,
                    new_tag=package in repo.new_archives,
                )
                runner.add_task(
                    tasks.PackageUploadTask(
                        task, wait_time=self.timeout, dry_run=dry_run, cleanup=True
                    )
                )

            self.errors.extend(repo.errors)

            # We can't actually push without auth
            if dry_run:
                logger.info(
                    f"Would push {repo.name} to {self.registry}, skipping for dry-run."
                )
                continue

            repo_runner.add_task(
                tasks.RepoUploadTask(repo, self.registry, cache_dir, dry_run)
            )

        self.errors.extend(runner.errors)
        items = runner.run_serial() if serial else runner.run()
        if not dry_run:
            items += repo_runner.run_serial() if serial else repo_runner.run()
        return items

    @decorators.require_registry
    def migrate_configs(self, dry_run=False, serial=False):
        """
        Replace the zero-byte config of published manifests with the empty descriptor.

        One task per repository: each package of the channel's repodata, yanked ones
        included, each --package given by exact name, plus the repodata and shards
        repositories of a complete mirror. The registry is not listed, so a package
        that upstream no longer has in its repodata is only reached by naming it.
        Manifests without the zero-byte config are skipped, so runs can be repeated
        and sliced with --subdir and --package.
        """
        self.errors.clear()
        util.print_item("Registry: ", self.registry)
        runner = tasks.TaskRunner(workers=self.workers)
        literal = {name for name in self.packages if not set("*?[") & set(name)}
        # Exact names are the repositories; only patterns and "all" need the repodata.
        need_repodata = not self.packages or literal != set(self.packages)

        @decorators.retry(attempts=3, giveup=decorators.is_permanent)
        def repodata_names(repo):
            return repo.load_repodata(include_yanked=True).package_names

        # The repodata only names the repositories: keep it out of the mirror's cache.
        with tempfile.TemporaryDirectory() as scratch:
            for subdir in self.subdirs:
                repo = repository.PackageRepo(
                    self.channel,
                    subdir,
                    os.path.join(scratch, subdir),
                    self.registry,
                    client=self.client,
                    mirror_shards=False,
                )
                names = literal | (repodata_names(repo) if need_repodata else set())
                uris = [
                    f"{self.registry}/{repo.name}/{pkg.package_reference(name)}"
                    for name in sorted(names)
                    if pkg.matches_package(name, self.packages)
                ]
                if not self.packages:
                    uris += [
                        f"{self.registry}/{repo.name}/repodata.json",
                        repo.shards_repository(self.registry),
                    ]
                for uri in uris:
                    runner.add_task(
                        tasks.ConfigRewriteTask(
                            uri, self.client, wait_time=self.timeout, dry_run=dry_run
                        )
                    )
        return runner.run_serial() if serial else runner.run()

    def iter_subdirs(self):
        """
        yield groups of channels, subdir, and cache directories.
        """
        for subdir in self.subdirs:
            cache_dir = os.path.join(self.cache_dir, self.channel, subdir)
            yield subdir, cache_dir

    @decorators.require_registry
    def pull_latest(self, dry_run=False, serial=False):
        """
        Pull latest packages from a location (the GitHub user) to a local cache.
        """
        self.errors.clear()
        util.print_item("From: ", self.registry)
        util.print_item("  To: ", self.cache_dir)

        # Create a task runner to do pulls
        runner = tasks.TaskRunner(workers=self.workers)

        for subdir, cache_dir in self.iter_subdirs():
            # Note that the original channel is relevant for a mirror
            uri = f"{self.registry}/{self.channel}/{subdir}/repodata.json:latest"

            # Create an empty repository data
            repodata = repository.RepoData()

            index_files = self.client.pull_by_media_type(
                uri, cache_dir, defaults.repodata_media_type_v1
            )
            if not index_files:
                raise ValueError(f"No repodata layer found for {uri}")
            repodata.load(index_files[0])
            logger.info(f"Found {len(repodata.package_archives)} packages from {uri}")

            for package_file, info in repodata.latest_packages(self.packages):
                media_type = repodata.get_package_mediatype(package_file)
                tag = f"{info['version']}-{info['build']}"
                if pkg.skip_invalid_tag(
                    tag, f"{self.channel}/{subdir}/{package_file}", self.errors
                ):
                    continue
                reference = pkg.package_reference(info["name"], tag)
                uri = f"{self.registry}/{self.channel}/{subdir}/{reference}"
                if dry_run:
                    logger.info(f"Would be pulling {reference}, but dry-run is set.")
                    continue
                runner.add_task(
                    tasks.DownloadTask(uri, cache_dir, media_type, client=self.client)
                )

        if serial:
            return runner.run_serial()
        return runner.run()

    @decorators.require_registry
    def push_all(self, dry_run=False, serial=False):
        """
        Push all packages to the remote.
        """
        return self.push(dry_run, push_all=True, serial=serial)

    @decorators.require_registry
    def push_new(self, dry_run=False, serial=False):
        """
        Push new packages to the remote.
        """
        return self.push(dry_run, push_all=False, serial=serial)

    @decorators.require_registry
    def push(self, dry_run=False, push_all=False, serial=False):
        """
        Push packages to the remote.
        """
        self.errors.clear()
        util.print_item("From: ", self.cache_dir)
        util.print_item("  To: ", self.registry)
        pushes = []

        for subdir, cache_dir in self.iter_subdirs():
            runner = tasks.TaskRunner(workers=self.workers)

            # Compare against the existing index without modifying the cache.
            repodata = repository.RepoData()
            repodata_file = os.path.join(cache_dir, "repodata.json")
            if os.path.exists(repodata_file):
                repodata.load(repodata_file)
            known_archives = set(repodata.package_archives)

            # Include both conda and tar.bz2 extension
            new_packages = []
            for ext in repository.package_extensions:
                files = list(pathlib.Path(cache_dir).rglob(f"*.{ext}"))
                if push_all:
                    new_packages += files
                else:
                    new_packages += [f for f in files if f.name not in known_archives]
            logger.info(f"Found {len(new_packages)} packages")

            # Push with an updated timestamp
            timestamp = datetime.datetime.now().astimezone().strftime("%Y.%m.%d.%H%M%S")

            # Upload new packages
            for package_name in new_packages:
                task = pkg.Package(
                    self.channel,
                    subdir,
                    package_name,
                    cache_dir,
                    registry=self.registry,
                    existing_file=str(package_name),
                    timestamp=timestamp,
                    client=self.client,
                )
                if not pkg.matches_package(task.package_name_bare, self.packages):
                    continue
                runner.add_task(
                    tasks.PackageUploadTask(
                        task, wait_time=self.timeout, dry_run=dry_run
                    )
                )

            self.errors.extend(runner.errors)

            # Run tasks for this runner
            if serial:
                pushes += runner.run_serial()
            else:
                pushes += runner.run()

        return pushes
