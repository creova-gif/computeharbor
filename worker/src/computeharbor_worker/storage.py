from __future__ import annotations

import importlib
import os
import shutil
from pathlib import Path
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class CheckpointStore(Protocol):
    """Protocol for pluggable object-store checkpoint replication."""

    def put_file(self, local_path: Path, key: str) -> str: ...

    def get_file(self, key: str, dest_path: Path) -> None: ...

    def exists(self, key: str) -> bool: ...


def _atomic_copy_file(src: Path, dst: Path) -> None:
    """Atomically copy *src* to *dst* via a sibling temp file, fsync, and os.replace."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dst.with_name(f".{dst.name}.{os.getpid()}.tmp")
    try:
        with open(src, "rb") as src_fh, open(tmp_path, "wb") as dst_fh:
            shutil.copyfileobj(src_fh, dst_fh)
            dst_fh.flush()
            os.fsync(dst_fh.fileno())
        os.replace(tmp_path, dst)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise


class LocalMirrorObjectStore:
    """Local filesystem mirror emulating a bucket-structured object store."""

    BUCKET_NAME = "computeharbor-ckpt"

    def __init__(self, root_dir: Path | str, scheme: str = "gs") -> None:
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self.scheme = scheme

    def _resolve_key(self, key: str) -> tuple[str, Path]:
        clean_key = key.lstrip("/")
        if not clean_key:
            raise ValueError("Object key must not be empty")
        rel_path = Path(clean_key)
        if rel_path.is_absolute() or ".." in rel_path.parts:
            raise ValueError(f"Invalid object key: {key!r}")
        return clean_key, self.root_dir / rel_path

    def put_file(self, local_path: Path, key: str) -> str:
        if not local_path.is_file():
            raise FileNotFoundError(f"Local checkpoint artifact not found: {local_path}")
        clean_key, target = self._resolve_key(key)
        _atomic_copy_file(local_path, target)
        return f"{self.scheme}://{self.BUCKET_NAME}/{clean_key}"

    def get_file(self, key: str, dest_path: Path) -> None:
        _, src = self._resolve_key(key)
        if not src.is_file():
            raise FileNotFoundError(f"Object not found in store: {key}")
        _atomic_copy_file(src, dest_path)

    def exists(self, key: str) -> bool:
        _, target = self._resolve_key(key)
        return target.is_file()


def cuda_synchronize_if_available() -> bool:
    """Synchronize CUDA device if PyTorch and a CUDA device are available."""
    try:
        torch_mod: Any = importlib.import_module("torch")
    except ImportError:
        return False

    cuda = getattr(torch_mod, "cuda", None)
    if cuda is None or not bool(cuda.is_available()):
        return False

    cuda.synchronize()
    return True
