# Conda OCI Mirror

Welcome to the Conda Forge Mirror python module! We use this module
to retrieve and update conda mirror packages. You can see it being used
in the wild [here](https://github.com/channel-mirrors/mirrormirror/blob/main/.github/workflows/main.yml)
and brief usage is shown below.

## Concepts

### Mirror

A **mirror** means you will copy (mirror) all packages from a regular conda-channel to an OCI registry.
In practice, this typically means **all** packages, however for the purposes of testing we allow selection of a subset.
You must have control of the registry you intend to mirror to, meaning you can push and pull from it.
When you do a mirror, the repodata.json is always pulled fresh, and any local changes you've made are
over-written. We do this so the local cache is in sync with the remote.

### Pull Cache

A **pull-cache** can pull from a registry that you may not be able to write to, to your local cache.

### Push Cache

A **push-cache** can push your local cache to a registry you control. This means that we compare packages you've
built against what are known in the repodata.json, and we push the ones that are not known to the repodata.json.
A push cache with the `--all` flag will push the entire contents of the local cache to your registry, regardless of
status.

## Usage

### Install

Create a new environment:

```bash
$ python -m venv env
$ source env/bin/activate
```

And install:

```bash
$ pip install -e .
```

### Authentication

You'll need an `ORAS_USER` and `ORAS_PASS` in the environment to be able
to push. You can also do a `--dry-run` to test out the library without pushing.
If you leave out dry run but don't have credentials, it will automatically be switched
to dry run.

```bash
export ORAS_USER=myuser
export ORAS_PASS=ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

Note that when you make the token, ensure the packages box (for read and write)
is checked.

### Mirror

The main functionality of the mirror is to create a copy of a channel and packages
and push them to a mirror (or just stage for a dry run). Let's say that we want to
mirror `conda-forge` and the package `zlib` We might first do a dry run:

```bash
$ conda-oci mirror --channel conda-forge --package zlib --dry-run
```

And then for realsies, here we want to push to our organization `myorg`

```bash
$ conda-oci mirror --channel conda-forge --package zlib --registry ghcr.io/myorg
```

For this command, we are pulling packages from a **channel** and mirroring to the
registry defined under **user**.

### Pull Cache

You can use `pull-cache` to pull the latest packages to a local cache.
The same authentication is needed only if the registry packages are
private or internal. Akin to the others, you can start with a `--dry-run`

```bash
$ conda-oci pull-cache --registry ghcr.io/researchapps --dry-run
```

Target a specific arch (subdir of the cache) and one package we know exist

```bash
$ conda-oci pull-cache --registry ghcr.io/researchapps --dry-run --subdir linux-64
```

If you want to preview what would be pulled, set `--dry-run`:

```bash
$ conda-oci pull-cache --registry ghcr.io/researchapps --subdir linux-64 --package zlib --dry-run
```

```console
Downloading conda-forge/linux-64/repodata.json to /home/vanessa/Desktop/Code/conda_oci_mirror/cache/conda-forge/linux-64/conda-forge/linux-64/repodata.json
Would be pulling zlib, but dry-run is set.
```

For this command, we are pulling packages from our registry defined as **user** and mirroring
to a local filesystem cache.

### Push Cache

You can use `push-cache` to push the packages in your cache to your remote.
This command will require authentication, and you are also encouraged to use
`--dry-run` first.

```bash
$ conda-oci push-cache --registry ghcr.io/researchapps --dry-run --package zlib --subdir linux-64
```

### Python API

#### Mirror

Let's say we want to do a simple mirror of a conda forge package to our own registry at `http://127.0.0.1:5000`.
Note that cache_dir will default to "cache" in your present working directory.

```python
from conda_oci_mirror.mirror import Mirror

mirror = Mirror(
    channel="conda-forge",
    packages=["redo"],

    # Push repodata and packages to this registry
    registry="http://127.0.0.1:5000/dinosaur",
    subdirs=["noarch"],
)

updates = mirror.update()
```

#### Pull Cache

A pull cache means that we start with a mirror, but then pull down packages from our registry
that aren't in our local cache. The creation of the Mirror is the same, except we call a different
function.

```python
from conda_oci_mirror.mirror import Mirror

mirror = Mirror(
    channel="conda-forge",
    packages=["redo"],

    # Push repodata and packages to this registry
    registry="http://127.0.0.1:5000/dinosaur",
    subdirs=["noarch"],
)

latest_packages = mirror.pull_latest()
```

### Push Cache

A push cache checks your local repodata.json and finds packages that exist that aren't yet added,
and then updates and pushes them to your registry. You can also use `push_all` to push all
local packages regardless of presence in the repodata.json. First, here is pushing new:

```python
from conda_oci_mirror.mirror import Mirror

mirror = Mirror(
    channel="conda-forge",
    packages=["redo"],

    # Push repodata and packages to this registry
    registry="http://127.0.0.1:5000/dinosaur",
    subdirs=["noarch"],
)

pushed_packages = mirror.push_new()
```

And pushing all:

```python
all_packages = mirror.push_all()
```

If you want to run in serial (for either of the above):

```python
all_packages = mirror.push_all(serial=True)
```

Right now this is checking against the local repodata.json, and I'm not sure if this should
be checking against the registry (intuitively it should if we expect a push-cache to push local cache
entries that aren't in the remote to the remote).

#### PackageRepo

You can use the `PackageRepo` class to interact directly with a package in a registry.
As an example, let's say we want to interact with a local registry `http://127.0.0.1/dinosaur`
to look for the `conda-forge` channel, `linux-64` subdirectory and package `zlib`.
You might first mirror the package there as follows:

```bash
$ conda-oci mirror --registry http://127.0.0.1:5000/dinosaur --subdir linux-64 --package zlib
```

You could also do this first with the Python API to get explicitly back the list of tags mirrored.
We would create a package repo as follows:

```python
from conda_oci_mirror.repo import PackageRepo
import os

cache_dir = os.path.join(os.getcwd(), 'cache')
repo = PackageRepo('conda-forge', 'linux-64', cache_dir, registry='http://127.0.0.1:5000/dinosaur')
```

Now let's retrieve the index.json. You need the exact tag you are interested in - there
is no "latest."

```python
# Should retrieve from
# http://127.0.0.1:5000/dinosaur/conda-forge/linux-64/zlib:1.2.11-0'
index_json = repo.get_index_json("zlib:1.2.11-0")
```

```
{'arch': 'x86_64',
 'build': '0',
 'build_number': 0,
 'depends': [],
 'license': 'zlib',
 'license_family': 'Other',
 'name': 'zlib',
 'platform': 'linux',
 'subdir': 'linux-64',
 'version': '1.2.11'}
```

Now we might want to get the package info:

```python
info = repo.get_info("zlib:1.2.11-0")
# <tarfile.TarFile at 0x7f7b78a00e80>
```

This is technically a tarfile, so to iterate over members:

```python
for member in info:
    print(member.name)
```

And finally, to get the full package archive:

```python
pkg = repo.get_package("zlib:1.2.11-0")
```

Note that we first try to get the new format (.conda) and fall back to .tar.bz2.

### Development

If you want to test pushing to packages, make sure to export your credentials first,
as discussed above. Then ensure that `--user` is targeting your GitHub user or organizational
account to push to:

```bash
$ conda-oci mirror --channel conda-forge --package zlib --registry ghcr.io/researchapps
```

For a package that includes the new format:

```bash
# Mirror zope.event from conda-forge to our local registry
$ conda-oci mirror --channel conda-forge --subdir noarch --package zope.event --registry http://127.0.0.0:5000/dinosaur

# Pull the package from our registry to out local cache
$ conda-oci pull-cache --channel conda-forge --subdir noarch --package zope.event --registry http://127.0.0.0:5000/dinosaur
```

You can also develop with a local registry (instead of ghcr.io):

```bash
$ docker run -it --rm -p 5000:5000 ghcr.io/oras-project/registry:latest
```

And then specify your local registry - oras
will fall back to insecure mode given that you've provided http://.

```bash
$ conda-oci mirror --channel conda-forge --package testinfra --registry http://127.0.0.0:5000/dinosaur --subdir noarch
```

And run tests:

```bash
$ pytest -xs conda_oci_mirror/tests/*.py
```

See [TODO.md](TODO.md) for some questions and items to do.

### Linting

We use pre-commit for linting. You can install dependencies and run it:

```bash
$ pip install -r .github/dev-requirements.txt
$ pre-commit run --all-files
```

Or install to the repository so it always runs before commit!

```bash
$ pre-commit install
```
