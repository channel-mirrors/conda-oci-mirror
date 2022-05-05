import fnmatch
import hashlib
import json

# import multiprocessing as mp
import os
import pathlib
import platform
import shutil
import subprocess
import tarfile
import time
from hmac import digest
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
    return tag.replace("+", "__p__").replace("!", "__e__")


def reverse_tag_format(tag):
    return tag.replace("__p__", "+").replace("__e__", "!")


def create_manifest(_layers,_repodata_dict,usr_org):
    url = f"https://ghcr.io/{usr_org}"
    manifest_dict = {"layers":[]}

    for layer in _layers:
        _media_type = layer.media_type
        _size = pathlib.Path(layer.file).stat().st_size
        _digest = "sha256:" + _repodata_dict["packages"][layer.file]["sha256"]

        infos = {"mediaType":_media_type,"size":_size,"digest":_digest}
        manifest_dict["layers"].append(infos)

    return manifest_dict

def sha256sum(path):
    hash_func = hashlib.sha256()

    with open(path, "rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            hash_func.update(byte_block)

    return hash_func.hexdigest()


def compute_hashlib(fn):
    BUF_SIZE = 65536
    curr_sha = hashlib.sha1()
    with open(fn, 'rb') as f:
        while True:
            _dt = f.read(BUF_SIZE)
            if not _dt:
                break
            curr_sha.update(_dt)
    return curr_sha.hexdigest()

def push_image(_base_path, oci,package, _layers):
    # manifest_dict = {"layers":[],"annotations":{}}

    manifest_dict = {"schemaVersion":2,"mediaType": "application/vnd.oci.image.manifest.v1+json","config":{}, "layers":[],"annotations":{}}


    
    gh_session = oci.oci_auth(package, scope="pull")
    pkg_name = package
    r = gh_session.post(f"https://ghcr.io/v2/{oci.user_or_org}/{pkg_name}/blobs/uploads/")
    headers = r.headers
    print (headers)
    location = headers['location']
    print (f"!! location: {location}")


    for layer in _layers:
        layer_path = _base_path / layer.file
        
        #update the manifest
        _media_type = layer.media_type
        _size = pathlib.Path(layer_path).stat().st_size
        digest = sha256sum(layer_path)
        _digest = "sha256:" + digest
        _hash_value = compute_hashlib(str(layer_path))
        infos = {"mediaType":_media_type,"size":_size,"digest":_digest, "hashlib":_hash_value}
        manifest_dict["layers"].append(infos)
        
        push_url = f"https://ghcr.io{location}?digest={_digest}"
        print (f"push url is : {push_url}")

        _headers = { "Content-Length": str(_size),"Content-Type": "application/octet-stream"}
        
        with open(str(layer_path), "rb") as f:
            r2 = gh_session.put(push_url, data=f, headers=_headers)
            print("+++++++++result")
            print(r2.content)
            print("+++end of result")
    
    manifest_dict["annotations"]["org.opencontainers.image.description"] = "start Description"
    manifest_path = _base_path / "manifest.json"
    print (f"!!! The path is {str(manifest_path)}")

    conf = {"mediaType": "application/vnd.oci.image.config.v1+json","size": 7023, "digest": "sha256:b5b2b2c507a0944348e0303114d8d93aaaa081732b86451d9bce1f432a537bc7"}
    manifest_dict ["config"] = conf


    with open(manifest_path, "w") as write_file:
        json.dump(manifest_dict, write_file)
    
    mnfst_size = pathlib.Path(manifest_path).stat().st_size
    mnfst_digest = sha256sum(manifest_path)
    _mnfst_digest = "sha256:" + mnfst_digest

    manifest_dict ["config"]["size"] = mnfst_size
    manifest_dict ["config"]["digest"] = _mnfst_digest
    
    with open(manifest_path, "w") as write_file:
        json.dump(manifest_dict, write_file)


    #_manfst_size = pathlib.Path(manifest_path).stat().st_size
    #_mnfst_headers = { "Content-Length": str(_manfst_size),"Content-Type": "application/json"}
    _mnfst_headers = { "Content-Type": "application/vnd.oci.image.manifest.v1+json"}
    ref = pkg_name + "-" + "latest"
    mnfst_url = f"https://ghcr.io/v2/{oci.user_or_org}/{pkg_name}/manifests/{ref}"
    
    with open(str(manifest_path), "rb") as f:
        r_manfst = gh_session.put(mnfst_url, data=f, headers=_mnfst_headers)
        print ("### result manifest")
        print (r_manfst.content)
        print ("### end of rslt")



        

    # push the manifest
    
    
def upload_conda_package(path_to_archive, host, channel, oci, extra_tags=None):
    path_to_archive = pathlib.Path(path_to_archive)
    package_name = get_package_name(path_to_archive)

    with TemporaryDirectory() as upload_files_directory:
        shutil.copy(path_to_archive, upload_files_directory)

        prepare_metadata(path_to_archive, upload_files_directory)

        if path_to_archive.name.endswith("tar.bz2"):
            layers = [Layer(path_to_archive.name, package_tarbz2_media_type)]
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

        oras = ORAS(base_dir=upload_files_directory)

        name = package_name.rsplit("-", 2)[0]
        version_and_build = tag_format("-".join(package_name.rsplit("-", 2)[1:]))

        with open(
            pathlib.Path(upload_files_directory) / package_name / "info" / "index.json",
            "r",
        ) as fi:
            j = json.load(fi)
            subdir = j["subdir"]

        print("Pushing: ", f"{host}/{channel}/{subdir}/{name}") 
        print (f"## Path dir is: {path_to_archive} ")
        prefix = str(path_to_archive).rsplit("/",1)[0]
        
        push_image(pathlib.Path(prefix), oci,name,layers)

        #oras.push(
        #    f"{host}/{channel}/{subdir}/{ngame}", version_and_build, layers + metadata
        #)

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


class Task:
    def __init__(self, channel, subdir, package, package_info, cache_dir, remote_loc):
        self.channel = channel
        self.subdir = subdir
        self.package = package
        self.packagel_info = package_info
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
                return self.retry(timeout=30, target_func=self.download_file)
            else:
                raise e

        return fn

    def retry(self, timeout=2, target_func=None):
        if not target_func:
            target_func = self.run

        print(f"Retrying in {timeout} seconds")
        time.sleep(timeout)
        self.retries += 1
        if self.retries > 3:
            raise RuntimeError(
                "Could not retrieve the correct file. Hashes not matching for 3 times"
            )

        return target_func()

    def run(self, oci):

        self.cache_dir.mkdir(parents=True, exist_ok=True)

        if not self.file or not self.file.exists():
            self.file = self.download_file()

        print(f"File downloaded: {self.file}")
        if check_checksum(self.file, self.packagel_info) is False:
            self.file.unlink()
            self.file = None
            return self.retry()

        try:
            upload_conda_package(self.file, self.remote_loc, self.channel, oci)
        except Exception:
            return self.retry()

        print(f"File uploaded to {self.remote_loc}")
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

    print("Cache dir is: ", cache_dir)
    raw_user_or_org = target_org_or_user.split(":")[1]
    oci = OCI("https://ghcr.io", raw_user_or_org)

    remote_loc = f"{host}/{raw_user_or_org}"

    tasks = []
    for channel in channels:
        for subdir in subdirs:

            full_cache_dir = cache_dir / channel / subdir

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
                    print("Adding task for ", key)
                    tasks.append(
                        Task(
                            channel,
                            subdir,
                            key,
                            package_info,
                            full_cache_dir,
                            remote_loc,
                        )
                    )
                # push_image(oci,key)

    if not dry_run:
        for task in tasks:
            start = time.time()
            task.run(oci)
            end = time.time()
            elapsed = end - start

            # This should at least take 20 seconds
            # Otherwise we sleep a bit
            if elapsed < 20:
                print("Sleeping for ", 20 - elapsed)
                time.sleep(20 - elapsed)

        # This was going too fast
        # with mp.Pool(processes=8) as pool:
        #     pool.map(run_task, tasks)


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
