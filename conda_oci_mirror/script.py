from conda_oci_mirror.repo import PackageRepo

# channel, subdir, cache_dir, registry=None):

repo = PackageRepo("conda-forge", "linux-64", "./cache", "ghcr.io/channel-mirrors")
info = repo.get_info("zlib:1.2.11-0")

names = info.getnames()
print(names)

f = info.extractfile("recipe/meta.yaml")
print(f.read())
