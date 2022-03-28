import os

def get_envvars(envvars):
    for e in envvars:
        if os.environ.get(e):
            return os.environ[e]
    return None

def get_github_auth(user=None):
    token = get_envvars(["GHA_PAT", "GITHUB_TOKEN"])
    user = user or get_envvars(["GHA_USER", "GITHUB_USER"])

    if user and token:
        return (user, token)

    return None
