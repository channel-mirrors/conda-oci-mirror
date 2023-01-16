## TODO

### High Priority

#### Support the new .conda package format.

> We do not yet support the "new" .conda package format in the OCI mirroring tools. Luckily the new conda-package-handling (and conda-package-streaming) packages are pretty OK and we can use them to do what we need: https://conda.github.io/conda-package-handling/api.html instead of using `tarfile`. The new package format files appear under the `packages.conda` key in the repodata.json file.

#### Deep and Shallow Modes

> currently we're checking against all tags on the ghcr registry which is a bit slow. We could have a "deep" and a "shallow" mode (where in the shallow mode it would use the "latest uploaded repodata" as reference from here: https://github.com/orgs/channel-mirrors/packages/container/package/conda-forge%2Fnoarch%2Frepodata.json

#### Compressed repodata

> It would be good to upload `repodata.json.zst` as a file compressed with zstd. In "regular" servers we ask for the gzip encoded response to get a compressed file over the wire but we need to be explicit with OCI registries as they don't support the on-the-fly encoding. Support for zst encoded repodata is being added to mamba soon.

### General

- [conda-package-handling](https://github.com/conda/conda-package-handling) is not installable via setup.cfg
- add better formatting for logger (colors?)
- add `--debug` mode to see what is happening at all steps
- It would be nice to have a version regular expression for those we want to mirror (there are often a lot). It's not obvious the best way to do this - since the user can specify multiple packages either we would have a package be like `--package zlib@1.2.11` or we would need to scope the action to be just for one package.

### Questions

- why is deid (and others I maintain) not in conda-forge noarch listing?
- I added size and creationTime annoations to layers - is that OK?
- why was repodata.json copied to original_repodata.json? Why do we need to save it (it doesn't seem to get used later)
- Can we have the push/pull-cache also be done in parallel?
- What does it mean for a package to start with an underscore (and in the code to change to `name = f"zzz{name}"`)?
