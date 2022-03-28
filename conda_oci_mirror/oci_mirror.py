import hashlib
import json
import os
import pathlib
import shutil
import subprocess
from tempfile import TemporaryDirectory

import requests
from conda_package_handling import api as cph_api

from conda_oci_mirror.constants import (
    CACHE_DIR,
    info_archive_media_type,
    info_index_media_type,
    package_conda_media_type,
    package_tarbz2_media_type,
)
from conda_oci_mirror.oci import OCI
from conda_oci_mirror.oras import ORAS, Layer
from conda_oci_mirror.util import get_github_auth


def compress_folder(source_dir, output_filename):
    return subprocess.run(
        f"tar -cvzf {output_filename} *",
        cwd=source_dir,
        shell=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=True,
    )


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


def upload_conda_package(path_to_archive, host, channel):
    path_to_archive = pathlib.Path(path_to_archive)
    package_name = get_package_name(path_to_archive)

    with TemporaryDirectory() as upload_files_directory:
        shutil.copy(path_to_archive, upload_files_directory)

        prepare_metadata(path_to_archive, upload_files_directory)

        if path_to_archive.name.endswith("tar.bz2"):
            layers = [Layer(path_to_archive.name, package_tarbz2_media_type)]
        else:
            layers = [Layer(path_to_archive.name, package_conda_media_type)]
        metadata = [
            Layer(f"{package_name}/info.tar.gz", info_archive_media_type),
            Layer(f"{package_name}/info/index.json", info_index_media_type),
        ]

        oras = ORAS(base_dir=upload_files_directory)

        name = package_name.rsplit("-", 2)[0]
        version_and_build = "-".join(package_name.rsplit("-", 2)[1:])

        with open(
            pathlib.Path(upload_files_directory) / package_name / "info" / "index.json",
            "r",
        ) as fi:
            j = json.load(fi)
            subdir = j["subdir"]

        print("Pushing: ", f"{host}/{channel}/{subdir}/{name}")
        oras.push(
            f"{host}/{channel}/{subdir}/{name}", version_and_build, layers + metadata
        )


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

    print("Auth: ", gh_session.auth)
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


def assert_checksum(path, package_dict):
    if "sha256" in package_dict:
        hash_func = hashlib.sha256()
        expected = package_dict["sha256"]
    elif "md5" in package_dict:
        hash_func = hashlib.md5()
        expected = package_dict["md5"]
    else:
        print("NO HASHES FOUND!")
        return

    with open(path, "rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            hash_func.update(byte_block)

    if hash_func.hexdigest() != expected:
        raise RuntimeError("HASHES ARE NOT MATCHING!")


def mirror(channels, subdirs, packages, target_org_or_user, host, cache_dir=None):
    if cache_dir is None:
        cache_dir = CACHE_DIR
    raw_user_or_org = target_org_or_user.split(":")[1]
    oci = OCI("https://ghcr.io", raw_user_or_org)

    remote_loc = f"{host}/{raw_user_or_org}"

    for channel in channels:
        for subdir in subdirs:
            repodata_fn = get_repodata(channel, subdir, cache_dir)

            existing_packages = set()

            all_subdir_packages = get_github_packages(
                "ghcr.io",
                target_org_or_user,
                filter_function=lambda x: x["name"].startswith(f"{channel}/{subdir}/"),
            )
            for gh_pkg in all_subdir_packages:
                for pkg in packages:
                    if gh_pkg["name"].endswith(f"/{pkg}"):
                        tags = oci.get_tags(gh_pkg["name"])
                        print(f"Found {len(tags)} existing tags for {gh_pkg['name']}")
                        existing_packages.update(
                            set(f"{pkg}-{tag}.tar.bz2" for tag in tags)
                        )

            with open(repodata_fn) as fi:
                j = json.load(fi)

            for key, package_info in j["packages"].items():

                if package_info["name"] in packages and key not in existing_packages:
                    r = requests.get(
                        f"https://conda.anaconda.org/{channel}/{subdir}/{key}",
                        allow_redirects=True,
                    )
                    full_cache_dir = cache_dir / channel / subdir
                    full_cache_dir.mkdir(parents=True, exist_ok=True)
                    ckey = full_cache_dir / key
                    with open(ckey, "wb") as fo:
                        fo.write(r.content)

                    assert_checksum(ckey, package_info)

                    upload_conda_package(ckey, remote_loc, channel)


if __name__ == "__main__":

    subdirs_to_mirror = [
        "linux-64",
        "osx-64",
        "osx-arm64",
        "win-64",
        "linux-aarch64",
        "linux-ppc64le",
        "noarch",
    ]
    mirror(
        ["conda-forge"], subdirs_to_mirror, ["xtensor", "pip"], "user:wolfv", "ghcr.io"
    )

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
