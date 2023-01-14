import datetime
import fnmatch
import hashlib
import json
import multiprocessing as mp
import os
import pathlib
import platform
import shutil
import subprocess
import tarfile
import time
from tempfile import TemporaryDirectory

import requests
from conda_package_handling import api as cph_api

from conda_oci_mirror.constants import (
    CACHE_DIR,
    info_archive_media_type,
    info_index_media_type,
    package_conda_media_type,
    package_tarbz2_media_type,
    repodata_v1,
)
from conda_oci_mirror.layer import Layer
from conda_oci_mirror.oci import OCI
from conda_oci_mirror.oras import ORAS
from conda_oci_mirror.util import get_github_auth, md5sum


def compress_folder(source_dir, output_filename):
    if not platform.system() == "Windows":
        return subprocess.run(
            f"tar -cvzf {output_filename} *",
            cwd=source_dir,
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
    else:
        with tarfile.open(output_filename, "w:gz") as tar:
            tar.add(source_dir, arcname=".")


def get_package_name(path_to_archive):
    fn = pathlib.Path(path_to_archive).name
    if fn.endswith(".tar.bz2"):
        return fn[:-8]
    elif fn.endswith(".conda"):
        return fn[:-6]
    else:
        raise RuntimeError("Cannot decipher package type")


def prepare_metadata(path_to_archive, upload_files_directory):
    package_name = get_package_name(path_to_archive)

    dest_dir = pathlib.Path(upload_files_directory) / package_name
    dest_dir.mkdir(parents=True)

    with TemporaryDirectory() as temp_dir:
        cph_api.extract(str(path_to_archive), temp_dir, components=["info"])
        index_json = os.path.join(temp_dir, "info", "index.json")
        info_archive = os.path.join(temp_dir, "info.tar.gz")
        compress_folder(
            os.path.join(temp_dir, "info"), os.path.join(temp_dir, "info.tar.gz")
        )

        (dest_dir / "info").mkdir(parents=True)
        shutil.copy(info_archive, dest_dir / "info.tar.gz")
        shutil.copy(index_json, dest_dir / "info" / "index.json")


def get_forbidden_packages():
    j = requests.get(
        "https://raw.githubusercontent.com/conda-forge/repodata-tools/main/repodata_tools/metadata.json"
    ).json()
    return j["undistributable"]


def tag_format(tag):
    return tag.replace("+", "__p__").replace("!", "__e__").replace("=", "__eq__")


def reverse_tag_format(tag):
    return tag.replace("__p__", "+").replace("__e__", "!").replace("__eq__", "=")


def upload_conda_package(path_to_archive, host, channel, oci, extra_tags=None):
    path_to_archive = pathlib.Path(path_to_archive)
    package_name = get_package_name(path_to_archive)

    layers = []
    with TemporaryDirectory() as upload_files_directory:
        upload_files_path = pathlib.Path(upload_files_directory)
        shutil.copy(path_to_archive, upload_files_directory)

        prepare_metadata(path_to_archive, upload_files_path)

        fn = upload_files_path / path_to_archive.name
        md5_value = md5sum(fn)

        _annotations_dict = {"org.conda.md5": md5_value}

        if path_to_archive.name.endswith("tar.bz2"):
            layers = [
                Layer(
                    path_to_archive.name, package_tarbz2_media_type, _annotations_dict
                )
            ]
        else:
            layers = [Layer(path_to_archive.name, package_conda_media_type)]

        # creation of info.tar.gz _does not yet work on windows_ properly...
        if platform.system() != "Windows":
            metadata = [
                Layer(f"{package_name}/info.tar.gz", info_archive_media_type),
                Layer(f"{package_name}/info/index.json", info_index_media_type),
            ]
        else:
            metadata = [Layer(f"{package_name}/info/index.json", info_index_media_type)]
        oras = ORAS(base_dir=upload_files_path)

        name = package_name.rsplit("-", 2)[0]
        version_and_build = tag_format("-".join(package_name.rsplit("-", 2)[1:]))

        with open(
            upload_files_path / package_name / "info" / "index.json",
            "r",
        ) as fi:
            j = json.load(fi)
            subdir = j.get("subdir")
            if not subdir:
                print("ERROR: info.json doesn't contain subdir!")
                return

        if name.startswith("_"):
            name = f"zzz{name}"

        print(f"Pushing: {host}/{channel}/{subdir}/{name}:{version_and_build}")
        oras.push(
            f"{host}/{channel}/{subdir}/{name}", version_and_build, layers + metadata
        )

        if extra_tags:
            for t in extra_tags:
                oras.push(f"{host}/{channel}/{subdir}/{name}", t, layers + metadata)

    return j


def get_repodata(channel, subdir, cache_dir=CACHE_DIR):
    repodata = cache_dir / channel / subdir / "repodata.json"
    if repodata.exists():
        return repodata
    repodata.parent.mkdir(parents=True, exist_ok=True)
    r = requests.get(
        f"https://conda.anaconda.org/{channel}/{subdir}/repodata.json",
        allow_redirects=True,
    )
    with open(repodata, "w") as fo:
        fo.write(r.text)
    return repodata


def get_github_packages(location, user_or_org, filter_function=None):

    gh_session = requests.Session()
    user_or_org, username_or_orgname = user_or_org.split(":")
    gh_session.auth = get_github_auth(username_or_orgname)

    # api_url = f'https://api.github.com/orgs/{org}/packages'
    headers = {"accept": "application/vnd.github.v3+json"}
    if user_or_org == "user":
        api_url = f"https://api.github.com/users/{username_or_orgname}/packages"
    elif user_or_org == "org":
        api_url = f"https://api.github.com/orgs/{username_or_orgname}/packages"

    api_url += "?package_type=container"  # could also add `visibility=public` here
    r = gh_session.get(api_url, headers=headers)

    packages = []
    if not filter_function:
        return r.json()

    for pkg in r.json():
        if filter_function(pkg):
            packages.append(pkg)

    return packages


def check_checksum(path, package_dict):
    if "sha256" in package_dict:
        hash_func = hashlib.sha256()
        expected = package_dict["sha256"]
    elif "md5" in package_dict:
        hash_func = hashlib.md5()
        expected = package_dict["md5"]
    else:
        print("NO HASHES FOUND!")
        return True

    with open(path, "rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            hash_func.update(byte_block)

    if hash_func.hexdigest() != expected:
        return False
    else:
        return True


existing_tags_cache = {}


def get_existing_tags(oci, channel, subdir, package):
    global existing_tags_cache

    if package.startswith("_"):
        package = f"zzz{package}"

    if package in existing_tags_cache:
        return existing_tags_cache[package]

    gh_name = f"{channel}/{subdir}/{package}"
    tags = oci.get_tags(gh_name)

    print(f"Found {len(tags)} existing tags for {gh_name}")
    tags = [reverse_tag_format(t) for t in tags]
    existing_tags_cache[package] = tags
    return tags


def get_existing_packages(oci, channel, subdir, package):
    tags = get_existing_tags(oci, channel, subdir, package)

    return set(f"{package}-{tag}.tar.bz2" for tag in tags)


package_counter = mp.Value("i", 0)
counter_start = mp.Value("d", time.time())
last_upload_time = mp.Value("d", time.time())


class Task:
    def __init__(
        self, oci, channel, subdir, package, package_info, cache_dir, remote_loc
    ):
        self.oci = oci
        self.channel = channel
        self.subdir = subdir
        self.package = package
        self.package_info = package_info
        self.cache_dir = cache_dir
        self.remote_loc = remote_loc
        self.retries = 0
        self.file = None

    def download_file(self):
        url = f"https://conda.anaconda.org/{self.channel}/{self.subdir}/{self.package}"
        fn = self.cache_dir / self.package

        try:
            with requests.get(url, stream=True, allow_redirects=True) as r:
                r.raise_for_status()
                with open(fn, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code >= 500:
                # todo check retry-after header
                return self.retry(
                    timeout=60,
                    target_func=self.download_file,
                    error="downloading file failed with >=500",
                )
            else:
                raise e

        return fn

    def retry(self, timeout=2, target_func=None, error="unspecified error"):
        if not target_func:
            target_func = self.run

        self.retries += 1

        t = timeout + 3**self.retries

        print(f"Retrying in {t} seconds - error: {error}")

        time.sleep(t)

        if self.retries > 5:
            raise RuntimeError(error)

        return target_func()

    def run(self):

        self.cache_dir.mkdir(parents=True, exist_ok=True)

        if not self.file or not self.file.exists():
            self.file = self.download_file()

        if check_checksum(self.file, self.package_info) is False:
            self.file.unlink()
            self.file = None
            return self.retry(error="checksums wrong")

        global package_counter, counter_start, last_upload_time

        with last_upload_time.get_lock():
            lt = last_upload_time.value
            tnow = time.time()
            rt = 0.5
            if tnow - lt < rt:
                print(f"Rate limit sleep for {(lt + rt) - tnow}")
                time.sleep((lt + rt) - tnow)
            last_upload_time.value = tnow

        try:
            upload_conda_package(self.file, self.remote_loc, self.channel, self.oci)
        except Exception:
            return self.retry(error="upload did not work")

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
        self.file.unlink()


def run_task(t):
    return t.run()


def mirror(
    channels, subdirs, packages, target_org_or_user, host, cache_dir=None, dry_run=False
):

    if cache_dir is None:
        cache_dir = CACHE_DIR

    if "conda-forge" in channels:
        forbidden_packages = get_forbidden_packages()
    else:
        forbidden_packages = []

    raw_user_or_org = target_org_or_user.split(":")[1]
    oci = OCI(host, raw_user_or_org)

    remote_loc = f"{host}/{raw_user_or_org}"

    tasks = []
    for channel in channels:
        for subdir in subdirs:

            full_cache_dir = cache_dir / channel / subdir

            repodata_timestamp = datetime.datetime.now()
            repodata_fn = get_repodata(channel, subdir, cache_dir)

            with open(repodata_fn) as fi:
                j = json.load(fi)

            for key, package_info in j["packages"].items():
                if packages:
                    if not any(
                        fnmatch.fnmatch(package_info["name"], x) for x in packages
                    ):
                        continue

                if package_info["name"] in forbidden_packages:
                    continue

                existing_packages = get_existing_packages(
                    oci, channel, subdir, package_info["name"]
                )

                if key not in existing_packages:
                    tasks.append(
                        Task(
                            oci,
                            channel,
                            subdir,
                            key,
                            package_info,
                            full_cache_dir,
                            remote_loc,
                        )
                    )
            repodata_layers = [Layer(repodata_fn.name, repodata_v1)]
            repodata_date_tag = repodata_timestamp.strftime("%Y.%m.%d.%H.%M")

            oras = ORAS(base_dir=full_cache_dir)
            print(
                f"Pushing repodata.json for {host}/{raw_user_or_org}/{channel}/{subdir}: {repodata_date_tag}"
            )

            oras.push(
                f"{host}/{raw_user_or_org}/{channel}/{subdir}/repodata.json",
                repodata_date_tag,
                repodata_layers,
            )
            print("Pushing latest tag.")
            oras.push(
                f"{host}/{raw_user_or_org}/{channel}/{subdir}/repodata.json",
                "latest",
                repodata_layers,
            )

    if not dry_run:
        global counter_start
        with counter_start.get_lock():
            counter_start.value = time.time()
        num_proc = 4
        # for task in tasks:
        #     # start = time.time()
        #     task.run()
        #     # end = time.time()
        #     # elapsed = end - start

        #     # This should at least take 20 seconds
        #     # Otherwise we sleep a bit
        #     if elapsed < 3:
        #         print("Sleeping for ", 3 - elapsed)
        #         time.sleep(3 - elapsed)

        # This was going too fast
        # with RateLimitedThreadPool(processes=num_proc, rate=30, per=60) as pool:
        with mp.Pool(processes=num_proc) as pool:
            pool.map(run_task, tasks)


if __name__ == "__main__":
    pass
    # subdirs_to_mirror = [
    #     "linux-64",
    #     "osx-64",
    #     "osx-arm64",
    #     "win-64",
    #     "linux-aarch64",
    #     "linux-ppc64le",
    #     "noarch",
    # ]
    # mirror(
    #     ["conda-forge"], subdirs_to_mirror, ["xtensor", "pip"], "user:wolfv", "ghcr.io"
    # )

    # oci = OCI('https://ghcr.io')
    # ns = "wolfv/conda-forge/osx-arm64/xtensor"
    # tags = oci.get_tags(ns)
    # for t in tags:
    #     print("Getting tag: ", t)
    #     manifest = oci.get_manifest(ns, t)

    #     for layer in manifest['layers']:
    #         if layer['mediaType'] == info_index_media_type:
    #             index_json_digest = layer['digest']
    #             break

    #     json_blob = oci.get_blob(ns, index_json_digest)
    #     print(json_blob.json())

    # oci.get_tags(""0.24.1-h3e96240_0")

    # for channel in channels_to_mirror:
    #     for subdir in subdirs_to_mirror:
    #         repodata_fn = get_repodata(channel, subdir)

    #         for pkg in packages_to_mirror:

    #             xtensor = get_github_packages('ghcr.io/wolfv', filter_function=lambda x: x['name'].startswith('osx-arm64/xtensor'))

    #             tags = get_package_tags('https://ghcr.io', 'wolfv/' + xtensor[0]['name'])
    #             print(tags)

    # pprint(xtensor)
    # exit(0)

    # with open(repodata_fn) as fi:
    #     j = json.load(fi)

    # for key, package in j["packages"].items():
    #     if package["name"] == 'xtensor':
    #         print("Loading ", key)

    #         r = requests.get(f"https://conda.anaconda.org/{channel}/{subdir}/{key}", allow_redirects=True)
    #         with open(key, 'wb') as fo:
    #             fo.write(r.content)
    #         upload_conda_package(key, 'ghcr.io/wolfv')

    # subdir = SubdirAccessor('ghcr.io/wolfv', 'osx-arm64')
    # index = subdir.get_index_json('xtensor-0.21.10-h260d524_0')
    # print(index)

    # with subdir.get_info('xtensor-0.21.10-h260d524_0') as fi:
    #     paths = json.load(fi.extractfile('paths.json'))
    #     print(paths)
