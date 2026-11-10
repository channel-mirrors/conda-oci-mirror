import hashlib

from conda_oci_mirror import defaults
from conda_oci_mirror.oras import ARTIFACT_TYPE, EMPTY_CONFIG, get_oras_client


def test_shard_pushes_reuse_the_token_and_the_config(
    token_registry, tmp_path, monkeypatch
):
    monkeypatch.setenv("ORAS_USER", "alice")
    monkeypatch.setenv("ORAS_PASS", "secret")
    client = get_oras_client(f"http://{token_registry['registry']}/test")
    basic = client.headers["Authorization"]

    def push(name):
        shard = tmp_path / f"{name}.msgpack.zst"
        shard.write_bytes(name.encode())
        client.push_single_layer(
            f"{token_registry['registry']}/test/shards:{name}",
            str(shard),
            defaults.repodata_shard_media_type_v1,
            f"shards/{shard.name}",
        )

    push("first")
    # 401, token, then config POST+PUT, blob POST+PUT, manifest PUT.
    assert len(token_registry["requests"]) == 7
    for name in ("second", "third"):
        push(name)
    # Every further shard is exactly blob POST, blob PUT, manifest PUT.
    assert len(token_registry["requests"]) == 7 + 2 * 3

    # An expired token is renewed with the credentials, not the stale token.
    token_registry["token"] = "t2"
    push("fourth")
    assert token_registry["requests"][-5:] == [
        ("POST", "/v2/test/shards/blobs/uploads/"),
        ("GET", "/token"),
        ("POST", "/v2/test/shards/blobs/uploads/"),
        ("PUT", "/v2/test/shards/blobs/uploads/1"),
        ("PUT", "/v2/test/shards/manifests/fourth"),
    ]

    manifest = token_registry["manifests"]["fourth"]
    (layer,) = manifest["layers"]
    assert layer["mediaType"] == defaults.repodata_shard_media_type_v1
    assert layer["digest"] == "sha256:" + hashlib.sha256(b"fourth").hexdigest()
    assert layer["annotations"]["org.opencontainers.image.title"] == (
        "shards/fourth.msgpack.zst"
    )
    assert manifest["config"] == EMPTY_CONFIG
    assert manifest["artifactType"] == ARTIFACT_TYPE
    assert token_registry["blobs"][EMPTY_CONFIG["digest"]] == b"{}"
    # The bearer token never leaks into the client's shared headers.
    assert client.headers["Authorization"] == basic
