# Conda OCI Mirror

Welcome to the Conda Forge Mirror python module! We use this module
to retrieve and update conda mirror packages. You can see it being used
in the wild [here](https://github.com/channel-mirrors/mirrormirror/blob/main/.github/workflows/main.yml)
and brief usage is shown below.

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

### Update

The main functionality of the mirror is to create a copy of a channel and packages
and push them to a mirror (or just stage for a dry run). Let's say that we want to
mirror `conda-forge` and the package `zlib` We might first do a dry run:

```bash
$ conda-oci mirror --channel conda-forge --package zlib --dry-run
```

And then for realsies:

```bash
$ conda-oci mirror --channel conda-forge --package zlib
```

### Development

If you want to test pushing to packages, make sure to export your credentials first,
as discussed above. Then ensure that `--user` is targeting your GitHub user or organizational
account to push to:

```bash
$ conda-oci mirror --channel conda-forge --package zlib --user researchapps
```

## TODO

- [conda-package-handling](https://github.com/conda/conda-package-handling) is not installable via setup.cfg
- ask wolf why deid (and others I maintain) not in conda-forge noarch listing?
- add better formatting for logger (colors?)
- add `--debug` mode to see what is happening at all steps
- Question: I added size and creationTime annoations to layers - is that OK?

Note that we aren't specifying any kind of credential or even subdirectory - we are just asking
to do a dry run for deid on conda-forge.

Notice that `--dry-run` is set, and this is appropriate because we haven't logged into
(or specified) a registry to push the mirror to.


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
