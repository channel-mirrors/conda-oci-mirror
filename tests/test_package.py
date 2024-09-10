from conda_oci_mirror.package import Package, _download_file_once


def test_package_download_fallback(tmp_path, monkeypatch):
    # Patch so we don't have to wait the retry decorator
    monkeypatch.setattr("conda_oci_mirror.package.download_file", _download_file_once)

    # This is a normal download via conda.anaconda.org
    package = Package(
        "conda-forge",
        "noarch",
        "verde-1.8.1-pyhd8ed1ab_0.conda",
        tmp_path,
        "ghcr.io/channel-mirrors",
    )
    package.ensure_file()

    # This one 404s on conda.anaconda.org, but should fallback to conda-web
    package = Package(
        "conda-forge",
        "osx-64",
        "arpack-3.6.1-blas_openblash1f444ea_0.tar.bz2",
        tmp_path,
        "ghcr.io/channel-mirrors",
    )
    package.ensure_file()
