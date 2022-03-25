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
