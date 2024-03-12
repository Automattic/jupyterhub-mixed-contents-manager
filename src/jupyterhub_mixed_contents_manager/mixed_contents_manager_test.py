import traitlets.config

import jupyterhub_mixed_contents_manager.mixed_contents_manager as mixed_contents_manager


def test_get_mount_point():
    mount_points = {
        "": "A",
        "b": "B",
        "b/c": "C",
    }
    checks = {
        "": "",
        "o": "",
        "b": "b",
        "b/o": "b",
        "b/c": "b/c",
        "b/c/o": "b/c",
    }
    for path, mount in checks.items():
        assert mixed_contents_manager.get_mount_point(mount_points, path) == mount


def test_mixed_contents_manager():
    # Couldn't manage to get more complicated testing due to traitlets issues, but this works at least:
    c = traitlets.config.Config()
    c.MixedContentsManager.mount_points_config = (
        ":::jupyter_server.services.contents.filemanager.FileContentsManager"
    )

    m = mixed_contents_manager.MixedContentsManager(config=c)

    assert m.get("")["content"][0]["name"] == "LICENSE"
