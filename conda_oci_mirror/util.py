import hashlib
import os


def get_envvars(envvars):
    for e in envvars:
        if os.environ.get(e):
            return os.environ[e]
    return None


def get_github_auth(user=None):
    token = get_envvars(["GHA_PAT", "GITHUB_TOKEN"])
    if token is None:
        raise RuntimeError(
            "Need to configure a github token (GHA_PAT or GITHUB_TOKEN environment variables)"
        )
    user = user or get_envvars(["GHA_USER", "GITHUB_USER"])
    print("User: ", user, " with token: ", (token is not None))

    if user and token:
        return (user, token)

    return None


def sha256sum(path):
    hash_func = hashlib.sha256()

    with open(path, "rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            hash_func.update(byte_block)

    return hash_func.hexdigest()


def md5sum(fn):
    curr_sha = hashlib.md5()
    with open(fn, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            curr_sha.update(byte_block)

    return curr_sha.hexdigest()
