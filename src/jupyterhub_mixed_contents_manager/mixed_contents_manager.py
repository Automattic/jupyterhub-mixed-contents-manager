"""A contents manager that combine multiple content managers."""

# Copyright (c) 2014, The Jupyter and IPython development team
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# Originally adapted from:
# https://github.com/jupyter/jupyter-drive/blob/master/jupyterdrive/mixednbmanager.py

from typing import Dict, Any
import copy
import pathlib

from jupyter_server.services.contents.manager import ContentsManager
from traitlets import traitlets, import_item

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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


def path_dispatch1(method):
    def f(self, path, *args, **kwargs):
        logger.debug(f"{method.__name__} {path}")
        manager, mount_point, child_path = self._path_lookup(path)
        logger.debug(
            f"path_dispatch1: {method.__name__} `{path}` -> mount: `{mount_point}` child_path: `{child_path}`"
        )
        return getattr(manager, method.__name__)(child_path, *args, **kwargs)

    return f


def path_dispatch2(method):
    def f(self, other, path, *args, **kwargs):
        manager, mount_point, child_path = self._path_lookup(path)
        logger.debug(
            f"path_dispatch2: {method.__name__} `{path}` -> mount: `{mount_point}` child_path: `{child_path}`"
        )
        return getattr(manager, method.__name__)(other, child_path, *args, **kwargs)

    return f


def path_dispatch_kwarg(method):
    def f(self, path=""):
        manager, mount_point, child_path = self._path_lookup(path)
        logger.debug(
            f"path_dispatch_kwarg: {method.__name__} `{path}` -> mount: `{mount_point}` child_path: `{child_path}`"
        )
        return getattr(manager, method.__name__)(path=child_path)

    return f


def path_dispatch_rename(method):
    """
    decorator for rename-like function, that need dispatch on 2 arguments
    """

    def f(self, path_a, path_b):
        manager_a, mount_point_a, child_path_a = self._path_lookup(path_a)
        manager_b, mount_point_b, child_path_b = self._path_lookup(path_b)
        logger.debug(
            f"path_dispatch_rename (arg a): {method.__name__} `{path_a}` -> mount: `{mount_point_a}` child_path: `{child_path_a}`"
        )
        logger.debug(
            f"path_dispatch_rename (arg b): {method.__name__} `{path_b}` -> mount: `{mount_point_b}` child_path: `{child_path_b}`"
        )

        if mount_point_a != mount_point_b:
            raise ValueError(
                "Does not know how to move things across contents manager mountpoints"
            )

        return getattr(manager_a, method.__name__)(child_path_a, child_path_b)

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
            mount_point: import_item(cls)(**kwargs)
            for mount_point, cls in parse_mount_points_config(
                self.mount_points_config
            ).items()
        }

    def _path_lookup(self, path: str):
        return path_lookup(self.mount_points_managers, path)

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
