import abc
import os
import typing
from pathlib import Path
import shutil


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
        with open(self._resolve_path(task_id, path), "rb") as f:
            return f.read()

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
            raise FileNotFoundError(f"Directory or file not found: {base}")
        return [str(p.relative_to(self.base_path / task_id)) for p in base.iterdir()]
