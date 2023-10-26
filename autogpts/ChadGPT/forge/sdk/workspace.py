import abc
import os
import typing
import logging
from pathlib import Path
import shutil
from .file_utils import extension_to_parser, read_textual_file, is_file_binary_fn

logger = logging.getLogger(__name__)


class Workspace(abc.ABC):

    def __init__(self, base_path: str) -> None:
        self.base_path = base_path

    @abc.abstractmethod
    def read(self, task_id: str, path: str) -> bytes:
        pass

    @abc.abstractmethod
    def write(self, task_id: str, path: str, data: bytes) -> None:
        pass

    @abc.abstractmethod
    def delete(
        self, task_id: str, path: str, directory: bool = False, recursive: bool = False
    ) -> None:
        pass

    @abc.abstractmethod
    def exists(self, task_id: str, path: str) -> bool:
        pass

    @abc.abstractmethod
    def list(self, task_id: str, path: str) -> typing.List[str]:
        pass


class LocalWorkspace(Workspace):

    def __init__(self, base_path: str):
        self.base_path = Path(base_path).resolve()

    def _resolve_path(self, task_id: str, path: typing.Union[str, Path]) -> Path:
        path = Path(path)
        if path.is_absolute():
            path = path.relative_to("/")
        abs_path = (self.base_path / task_id / path).resolve()

        if self.base_path not in abs_path.parents:
            raise ValueError(f"Directory traversal is not allowed! - {abs_path}")

        abs_path.parent.mkdir(parents=True, exist_ok=True)
        return abs_path

    def read(self, task_id: str, path: str) -> bytes:
        abs_path = self._resolve_path(task_id, path)

        try:
            # Check if the file extension is recognized by the utility parsers
            if abs_path.suffix.lower() in extension_to_parser or not is_file_binary_fn(abs_path):
                return read_textual_file(abs_path, logger).encode('utf-8')
            # For non-textual or unrecognized formats, read as binary
            else:
                with open(abs_path, "rb") as f:
                    return f.read()
        except Exception as e:
            logger.error(f"Error reading file {abs_path}: {e}")
            raise e

    def write(self, task_id: str, path: str, data: bytes) -> None:
        with open(self._resolve_path(task_id, path), "wb") as f:
            f.write(data)

    def delete(
        self, task_id: str, path: str, directory: bool = False, recursive: bool = False
    ) -> None:
        resolved_path = self._resolve_path(task_id, path)
        if directory:
            if recursive:
                shutil.rmtree(resolved_path)
            else:
                os.rmdir(resolved_path)
        else:
            os.remove(resolved_path)

    def exists(self, task_id: str, path: str) -> bool:
        return self._resolve_path(task_id, path).exists()

    def list(self, task_id: str, path: str) -> typing.List[str]:
        base = self._resolve_path(task_id, path)
        if not base.exists():
            return []  # Return an empty list if the directory doesn't exist
        return [str(p.relative_to(self.base_path / task_id)) for p in base.iterdir()]

