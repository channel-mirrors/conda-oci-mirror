# cache packages -- first pull all packages with `latest` tag,
# then push new packages
import json
import subprocess
from datetime import datetime
from pathlib import Path

from conda_oci_mirror.constants import package_tarbz2_media_type
from conda_oci_mirror.layer import Layer
from conda_oci_mirror.oci_mirror import upload_conda_package
from conda_oci_mirror.oras import ORAS


def load_json(path):
    with open(path) as fi:
        j = json.load(fi)
    return j


def pull_latest_packages(location, packages, subdirs, cache_dir, dry_run=False):
    cache_dir = Path(cache_dir)

    for subdir in subdirs:
        sdir_cache = cache_dir / subdir
        sdir_cache.mkdir(parents=True, exist_ok=True)

        oras = ORAS(base_dir=sdir_cache)

        try:
            oras.pull_tag(
                location, subdir, "repodata.json", "latest", "application/json"
            )

            repodata = load_json(sdir_cache / "repodata.json")
            print(repodata["packages"])
            packages = set([p["name"] for k, p in repodata["packages"].items()])

            (sdir_cache / "repodata.json").rename(sdir_cache / "original_repodata.json")

        except Exception as e:
            print(e)

        for p in packages:
            print("Pulling ", location, subdir, p, "latest")
            oras.pull_tag(location, subdir, p, "latest", package_tarbz2_media_type)


# call conda index to create updated repodata.json
def conda_index(cache_dir):
    subprocess.check_output(["conda", "index", str(cache_dir)])


def push_new_packages(location, packages, subdirs, cache_dir, dry_run=False):
    cache_dir = Path(cache_dir)
    conda_index(cache_dir)

    now = datetime.now()

    date_time_tag = now.strftime("%Y.%m.%d.%H%M%S")

    for subdir in subdirs:

        sdir_cache = cache_dir / subdir

        sdir_cache.mkdir(parents=True, exist_ok=True)

        orig_repodata = sdir_cache / "original_repodata.json"

        if orig_repodata.exists():
            repodata = load_json(orig_repodata)
        else:
            repodata = {"packages": []}

        files = list(Path(sdir_cache).rglob("*.tar.bz2"))
        print(files)
        new_packages = []
        for f in files:
            print(f.name)
            if f.name not in repodata["packages"]:
                new_packages.append(f)

        for p in new_packages:
            host, channel = location.rsplit("/", 1)
            print("Uploading ", p, host, channel)

            upload_conda_package(p, host, channel, extra_tags=["latest"])

        layers = [Layer("repodata.json", "application/json")]
        oras = ORAS(base_dir=sdir_cache)
        oras.push(f"{location}/{subdir}/repodata.json", date_time_tag, layers)
        oras.push(f"{location}/{subdir}/repodata.json", "latest", layers)
