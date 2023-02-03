## TODO

### High Priority

#### Deep and Shallow Modes

> currently we're checking against all tags on the ghcr registry which is a bit slow. We could have a "deep" and a "shallow" mode (where in the shallow mode it would use the "latest uploaded repodata" as reference from here: https://github.com/orgs/channel-mirrors/packages/container/package/conda-forge%2Fnoarch%2Frepodata.json

#### Compressed repodata

> It would be good to upload `repodata.json.zst` as a file compressed with zstd. In "regular" servers we ask for the gzip encoded response to get a compressed file over the wire but we need to be explicit with OCI registries as they don't support the on-the-fly encoding. Support for zst encoded repodata is being added to mamba soon.

### General

- It would be nice to have a version regular expression for those we want to mirror (there are often a lot). It's not obvious the best way to do this - since the user can specify multiple packages either we would have a package be like `--package zlib@1.2.11` or we would need to scope the action to be just for one package.

### Questions

- If we are doing a mirror and selecting a particular package(s) - why do we update all repodata to include all packages even if a package of interest does not exist? What should be the default?git
