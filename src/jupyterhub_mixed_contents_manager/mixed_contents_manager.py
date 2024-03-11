"""A contents manager that combine multiple content managers."""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

# File adapted from:
# https://github.com/jupyter/jupyter-drive/blob/master/jupyterdrive/mixednbmanager.py

from typing import Dict, Any
import copy
import pathlib

from jupyter_server.services.contents.manager import ContentsManager
from traitlets import traitlets, import_item


def parse_mount_points_config(conf: str) -> Dict[str, str]:
    """
    e.g. hdfs:::hdfscm.HDFSContentsManager,:::jupyter_server.services.contents.filemanager.FileContentsManager

    Note that leading/trailing slashes are omitted.
    """
    if conf:
        return {
            path_class.split(":::")[0]: path_class.split(":::")[1]
            for path_class in conf.split(",")
        }
    else:
        return {}


def get_mount_point(mount_points_dict: Dict[str, Any], path: str):
    return next(
        mount_point
        for mount_point in sorted(mount_points_dict.keys(), reverse=True)
        if pathlib.PurePath(f"/{path}").is_relative_to(
            pathlib.PurePath(f"/{mount_point}")
        )
    )


def get_child_path(mount_point: str, path: str):
    return str(
        pathlib.PurePath(f"/{path}").relative_to(pathlib.PurePath(f"/{mount_point}"))
    )


def path_lookup(mount_points_managers: Dict[str, Any], path: str):
    "returns manager, mount_point, child_path"
    mount_point = get_mount_point(mount_points_managers, path)
    return (
        mount_points_managers[mount_point],
        mount_point,
        get_child_path(mount_point, path),
    )


def path_dispatch1(method_name):
    def f(self, path, *args, **kwargs):
        manager, _mount_point, child_path = self._path_lookup(path)
        return getattr(manager, method_name)(child_path, *args, **kwargs)

    return f


def path_dispatch2(method_name):
    def f(self, other, path, *args, **kwargs):
        manager, _mount_point, child_path = self._path_lookup(path)
        return getattr(manager, method_name)(other, child_path, *args, **kwargs)

    return f


def path_dispatch_kwarg(method_name):
    def f(self, path=""):
        manager, _mount_point, child_path = self._path_lookup(path)
        if manager is not None:
            return getattr(manager, method_name)(path=child_path)

    return f


def path_dispatch_rename(method):
    """
    decorator for rename-like function, that need dispatch on 2 arguments
    """

    def f(self, path_a, path_b):
        manager_a, mount_point_a, child_path_a = self._path_lookup(path_a)
        manager_b, mount_point_b, child_path_b = self._path_lookup(path_b)

        if mount_point_a != mount_point_b:
            raise ValueError(
                "Does not know how to move things across contents manager mountpoints"
            )

        if manager_a is not None:
            return getattr(manager_a, method.__name__)(child_path_a, child_path_b)
        else:
            return method(self, path_a, path_b)

    return f


class MixedContentsManager(ContentsManager):
    mount_points_config = traitlets.Unicode(
        "",
        help="""
        Format: mount/path:::contents_manager_class[,...]
        e.g. hdfs:::hdfscm.HDFSContentsManager,:::jupyter_server.services.contents.filemanager.FileContentsManager

        Note that leading/trailing slashes are omitted, so root '/' becomes '' 
        """,
        config=True,
    )

    def __init__(self, **kwargs):
        super(MixedContentsManager, self).__init__(**kwargs)
        kwargs.update({"parent": self})
        self.mount_points_managers = {
            mount_point: import_item(cls)
            for mount_point, cls in parse_mount_points_config(
                ":::jupyter_server.services.contents.filemanager.FileContentsManager,hdfs-home:::hdfscm.HDFSContentsManager",
                # self.mount_points_config
            ).items()
        }
        assert self.path_lookup("")[1] == ""
        assert self.mount_points_managers[""], "Root mount point required"
        assert self.mount_points_managers["hdfs-home"]
        assert self.get("")

    def _path_lookup(self, path: str):
        return path_lookup(self.mount_point_managers, path)

    @path_dispatch1
    def dir_exists(self, path):
        raise NotImplementedError()

    @path_dispatch1
    def is_hidden(self, path):
        raise NotImplementedError()

    @path_dispatch_kwarg
    def file_exists(self, path=""):
        raise NotImplementedError()

    @path_dispatch1
    def exists(self, path):
        raise NotImplementedError()

    @path_dispatch1
    def get(self, path, **kwargs):
        raise NotImplementedError()

    @path_dispatch2
    def save(self, model, path):
        raise NotImplementedError()

    def update(self, model, path):
        manager_a, mount_point_a, child_path_a = self._path_lookup(path)
        _manager_b, mount_point_b, child_path_b = self._path_lookup(model["path"])

        if mount_point_a != mount_point_b:
            raise ValueError("Cannot move files across mount points")

        new_model = copy.deepcopy(model)
        new_model["path"] = child_path_b

        return getattr(manager_a, "update")(model, child_path_a)

    @path_dispatch1
    def delete(self, path):
        raise NotImplementedError()

    @path_dispatch1
    def create_checkpoint(self, path):
        raise NotImplementedError()

    @path_dispatch1
    def list_checkpoints(self, path):
        raise NotImplementedError()

    @path_dispatch2
    def restore_checkpoint(self, checkpoint_id, path):
        raise NotImplementedError()

    @path_dispatch2
    def delete_checkpoint(self, checkpoint_id, path):
        raise NotImplementedError()

    @path_dispatch_rename
    def rename_file(self, old_path, new_path):
        raise NotImplementedError()

    @path_dispatch_rename
    def rename(self, old_path, new_path):
        raise NotImplementedError()
