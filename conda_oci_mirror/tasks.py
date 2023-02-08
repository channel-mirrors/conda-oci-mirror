import hashlib
import multiprocessing as mp
import os
import time

import requests
import xattr
import yaml

import conda_oci_mirror.package as pkg
from conda_oci_mirror.logger import logger
from conda_oci_mirror.oras import oras

# Counters for lifetime of tasks
package_counter = mp.Value("i", 0)
counter_start = mp.Value("d", time.time())
last_upload_time = mp.Value("d", time.time())


class TaskBase:
    """
    Shared task base for all tasks
    """

    def wait(self):
        """
        Wait the appropriate timeout for the last upload time.
        """
        global last_upload_time

        with last_upload_time.get_lock():
            lt = last_upload_time.value
            now = time.time()
            # Slighly slower than the original 0.5 rate limit
            rt = 0.25
            if now - lt < rt:
                print(f"Rate limit sleep for {(lt + rt) - now}")
                time.sleep((lt + rt) - now)
            last_upload_time.value = now


class RepoUploadTask(TaskBase):
    """
    The RepoUploadTask wraps a repository to upload a cache-dir to a registry.
    """

    def __init__(self, repo, registry, cache_dir, dry_run=False):
        self.repo = repo
        self.registry = registry
        self.cache_dir = cache_dir
        self.dry_run = dry_run

    def run(self):
        """
        Run the repo task, uploading the data and taking a pause if needed.
        """
        global package_counter, counter_start

        # Wait based on the last upload time across tasks
        self.wait()

        # This has retry wrapper - we get back metadata about the package pushed
        return self.repo.upload(self.cache_dir, registry=self.registry)


class PackageUploadTask(TaskBase):
    """
    A single task to upload a package, and cleanup.
    """

    def __init__(self, pkg, dry_run=False):
        self.dry_run = dry_run
        self.pkg = pkg

    def run(self):
        """
        Run the task. This means:

        1. Creating the package-specific cache directory.
        2. Downloading the current file.
        3. Upload the package (or emulating it)
        """
        self.pkg.ensure_file()

        global package_counter, counter_start

        # Wait based on the last upload time across tasks
        self.wait()

        # This has retry wrapper - we get back metadata about the package pushed
        result = self.pkg.upload(self.dry_run)

        with package_counter.get_lock(), counter_start.get_lock():
            package_counter.value += 1
            if package_counter.value % 10 == 0:
                elapsed_min = (time.time() - counter_start.value) / 60.0
                print(
                    "Average no packages / min: ", package_counter.value / elapsed_min
                )

            if package_counter.value % 50 == 0:
                package_counter.value = 0
                counter_start.value = time.time()

        # delete the package
        self.pkg.delete()
        return result


class SourceDownloadTask(TaskBase):
    """
    A simple task to download a source package
    """

    def __init__(self, name, info, repo, cache_dir):
        self.pkg_name = name
        self.info = info
        self.repo = repo
        self.cache_dir = os.path.join(cache_dir, name)
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

    def _download_source(self, source):
        """
        Download a single source
        """
        # Wait based on the last interaction time
        self.wait()

        if "url" not in source:
            return

        url = source.get("url")
        sha256 = source.get("sha256")
        if not sha256:
            raise Exception(f"Cannot pull package {url}: no sha256")

        # get extension, can be .tar.gz, .tar.bz2, .zip, etc.
        rest, ext = os.path.splitext(url)
        tar_ext = os.path.splitext(rest)[1]
        if tar_ext == ".tar":
            ext = tar_ext + ext

        fn = os.path.join(self.cache_dir, sha256 + ext)

        try:
            request = requests.get(url, stream=True)
            request.raise_for_status()
            with open(fn, "wb") as f:
                for chunk in request.iter_content(chunk_size=8192):
                    f.write(chunk)

            # check sha256 of the file
            with open(fn, "rb") as f:
                file_sha256 = hashlib.sha256(f.read()).hexdigest()
                if file_sha256 != sha256:
                    os.remove(fn)
                    raise Exception(
                        f"File {url} has sha256 {file_sha256} but expected {sha256}"
                    )

                x = xattr.xattr(f)
                x.set("user.source.url", url.encode("utf-8"))
        except Exception as e:
            logger.warning(f"Cannot pull package {url}: {e}")

    def _get_info(self):
        try:
            info = self.info
            name, version, build = info["name"], info["version"], info["build"]
            tag = pkg.version_build_tag(f"{version}-{build}")
            info = self.repo.get_info(f"{name}:{tag}")
        except Exception as e:
            logger.warning(f"info not extractable, skipping {self.pkg_name} {e}")
            return False

        try:
            f = info.extractfile("recipe/meta.yaml")
        except Exception as e:
            logger.warning(f"recipe not extractable, skipping {self.pkg_name} {e}")
            return False

        try:
            y = yaml.safe_load(f)
        except yaml.YAMLError as exc:
            logger.warning(f"recipe not parseable, skipping {self.pkg_name} {exc}")
            return False

        if "source" in y:
            self.sources = y["source"]
            logger.info(
                f"{y['package']['name']} {y['package']['version']}: {self.sources}"
            )
            return True
        else:
            logger.warning(f"recipe has no source, skipping {self.pkg_name}")
            return False

    def run(self):
        """
        Run the task to download the package
        """
        # Wait based on the last interaction time
        self.wait()
        has_info = self._get_info()
        if not has_info:
            logger.warning(f"Cannot pull package {self.pkg_name}: Could not load info")
            return
        try:
            if isinstance(self.sources, list):
                for source in self.sources:
                    self._download_source(source)
            else:
                self._download_source(self.sources)
        except Exception as e:
            logger.warning(f"Cannot pull package {self.pkg_name}: {e}")


class DownloadTask(TaskBase):
    """
    A simple task to download a blob / media type
    """

    def __init__(self, uri, cache_dir, media_type):
        self.uri = uri
        self.cache_dir = cache_dir
        self.media_type = media_type

    def run(self):
        """
        Run the task to download the package
        """
        # Wait based on the last interaction time
        self.wait()

        try:
            return oras.pull_by_media_type(self.uri, self.cache_dir, self.media_type)
        except Exception as e:
            logger.warning(f"Cannot pull package {self.uri}: {e}")


class TaskRunner:
    """
    A task runner knows how to time and run tasks!
    """

    def __init__(self, workers=4):
        self.workers = workers
        self.tasks = []

    def add_task(self, task):
        self.tasks.append(task)

    def run_serial(self):
        """
        Run tasks in serial (this is intended for debugging mostly)
        """
        global counter_start
        with counter_start.get_lock():
            counter_start.value = time.time()

        # Keep track of results
        items = []
        for task in self.tasks:
            start = time.time()
            result = task.run()
            if isinstance(result, list):
                items += result
            else:
                items.append(result)
            end = time.time()
            elapsed = end - start

            # This should at least take 20 seconds
            # Otherwise we sleep a bit
            if elapsed < 3:
                print("Sleeping for ", 3 - elapsed)
                time.sleep(3 - elapsed)

        return items

    def run(self):
        """
        Run the tasks!
        """
        global counter_start
        with counter_start.get_lock():
            counter_start.value = time.time()

        # Keep track of results
        items = []
        with mp.Pool(processes=self.workers) as pool:
            for result in pool.map(run_task, self.tasks):
                # This is a smaller list of packages/repo metadata pushes
                if isinstance(result, list):
                    items += result
                else:
                    items.append(result)

        # Return all results from running the task
        return items


def run_task(t):
    """
    Anything with a run function can be provided as a task.
    """
    return t.run()
