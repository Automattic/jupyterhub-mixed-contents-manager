"""A contents manager that combine multiple content managers."""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

# File adapted from:
# https://github.com/jupyter/jupyter-drive/blob/master/jupyterdrive/mixednbmanager.py

from typing import Dict
import pathlib

from jupyter_server.services.contents.manager import ContentsManager
from traitlets import traitlets, import_item


def parse_mount_points_config(conf: str) -> Dict[str, str]:
    "e.g. /hdfs:::hdfscm.HDFSContentsManager,/:::jupyter_server.services.contents.filemanager.FileContentsManager"
    if conf:
        return {
            path_class.split(":::")[0]: path_class.split(":::")[1]
            for path_class in conf.split(",")
        }
    else:
        return {}


class MixedContentsManager(ContentsManager):
    mount_points_config = traitlets.Unicode(
        "", help="mount/path:::contents_manager_class[,...]", config=True
    )

    def __init__(self, **kwargs):
        super(MixedContentsManager, self).__init__(**kwargs)
        kwargs.update({"parent": self})
        self.mount_points_managers = {
            mount_point: import_item(cls)
            for mount_point, cls in parse_mount_points_config(
                self.mount_points_config
            ).items()
        }

    def get_mount_point(self, path: str):
        return next(
            (
                mount_point
                for mount_point in sorted(self.mount_points_managers.keys())
                if pathlib.PurePath(path).is_relative_to(pathlib.PurePath(mount_point))
            ),
            None,
        )

    def get_child_path(self, mount_point: str, path: str):
        return str(pathlib.PurePath(path).relative_to(pathlib.PurePath(mount_point)))

    def path_lookup(self, path: str):
        "returns manager, mount_point, child_path"
        mount_point = self.get_mount_point(path)
        if mount_point:
            return (
                self.mount_points_managers.get(mount_point),
                mount_point,
                self.get_child_path(mount_point, path),
            )
        return None, None, None

    def path_dispatch1(method):
        def _wrapper_method(self, path, *args, **kwargs):
            manager, _mount_point, child_path = self.path_lookup(path)
            if manager is not None:
                return getattr(manager, method.__name__)(child_path, *args, **kwargs)
            else:
                return method(self, path, *args, **kwargs)

        return _wrapper_method

    def path_dispatch2(method):
        def _wrapper_method(self, other, path, *args, **kwargs):
            manager, _mount_point, child_path = self.path_lookup(path)
            if manager is not None:
                return getattr(manager, method.__name__)(
                    other, child_path, *args, **kwargs
                )
            else:
                return method(self, other, path, *args, **kwargs)

        return _wrapper_method

    def path_dispatch_kwarg(method):
        def _wrapper_method(self, path=""):
            manager, _mount_point, child_path = self.path_lookup(path)
            if manager is not None:
                return getattr(manager, method.__name__)(path=child_path)
            else:
                return method(self, path=path)

        return _wrapper_method

    def path_dispatch_rename(method):
        """
        decorator for rename-like function, that need dispatch on 2 arguments
        """

        def _wrapper_method(self, path_a, path_b):
            manager_a, mount_point_a, child_path_a = self.path_lookup(path_a)
            manager_b, mount_point_b, child_path_b = self.path_lookup(path_b)

            if mount_point_a != mount_point_b:
                raise ValueError(
                    "Does not know how to move things across contents manager mountpoints"
                )

            if manager_a is not None:
                return getattr(manager_a, method.__name__)(child_path_a, child_path_b)
            else:
                return method(self, path_a, path_b)

        return _wrapper_method

    @path_dispatch1
    def dir_exists(self, path):
        # root exists
        if len(path) == 0:
            return True
        if path in self.mount_points_managers.keys():
            return True
        return False

    @path_dispatch1
    def is_hidden(self, path):
        if (len(path) == 0) or path in self.mount_points_managers.keys():
            return False
        raise NotImplementedError("...." + path)

    @path_dispatch_kwarg
    def file_exists(self, path=""):
        if len(path) == 0:
            return False
        raise NotImplementedError("NotImplementedError")

    @path_dispatch1
    def exists(self, path):
        if len(path) == 0:
            return True
        raise NotImplementedError("NotImplementedError")

    @path_dispatch1
    def get(self, path, **kwargs):
        if len(path) == 0:
            return [{"type": "directory"}]
        raise NotImplementedError("NotImplementedError")

    @path_dispatch2
    def save(self, model, path):
        raise NotImplementedError("NotImplementedError")

    def update(self, model, path):
        manager_a, mount_point_a, child_path_a = self.path_lookup(path)
        _manager_b, mount_point_b, child_path_b = self.path_lookup(model["path"])

        if mount_point_a != mount_point_b:
            raise ValueError("Cannot move files across mount points")

        model["path"] = child_path_b

        if manager_a is not None:
            return getattr(manager_a, "update")(model, child_path_a)
        else:
            return self.method(model, path)

    @path_dispatch1
    def delete(self, path):
        raise NotImplementedError("NotImplementedError")

    @path_dispatch1
    def create_checkpoint(self, path):
        raise NotImplementedError("NotImplementedError")

    @path_dispatch1
    def list_checkpoints(self, path):
        raise NotImplementedError("NotImplementedError")

    @path_dispatch2
    def restore_checkpoint(self, checkpoint_id, path):
        raise NotImplementedError("NotImplementedError")

    @path_dispatch2
    def delete_checkpoint(self, checkpoint_id, path):
        raise NotImplementedError("NotImplementedError")

    @path_dispatch_rename
    def rename_file(self, old_path, new_path):
        """Rename a file."""
        raise NotImplementedError("must be implemented in a subclass")

    @path_dispatch_rename
    def rename(self, old_path, new_path):
        """Rename a file."""
        raise NotImplementedError("must be implemented in a subclass")
