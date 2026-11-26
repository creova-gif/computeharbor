from __future__ import annotations

import importlib
import os
import shutil
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class CheckpointStore(Protocol):
    """Protocol for pluggable object-store checkpoint replication."""

    def put_file(self, local_path: Path, key: str) -> str: ...

    def get_file(self, key: str, dest_path: Path) -> None: ...

    def exists(self, key: str) -> bool: ...


def _validate_object_key(key: str) -> str:
    """Validate an object key against empty values and path traversal."""
    clean_key = key.lstrip("/")
    if not clean_key:
        raise ValueError("Object key must not be empty")
    rel_path = Path(clean_key)
    if rel_path.is_absolute() or ".." in rel_path.parts:
        raise ValueError(f"Invalid object key: {key!r}")
    return clean_key


def _atomic_write_from_stream(src_fh: Any, dst: Path) -> None:
    """Atomically write *src_fh* to *dst* via a sibling temp file, fsync, and os.replace."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dst.with_name(f".{dst.name}.{os.getpid()}.tmp")
    try:
        with open(tmp_path, "wb") as dst_fh:
            shutil.copyfileobj(src_fh, dst_fh)
            dst_fh.flush()
            os.fsync(dst_fh.fileno())
        os.replace(tmp_path, dst)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise


def _atomic_copy_file(src: Path, dst: Path) -> None:
    """Atomically copy *src* to *dst* via a sibling temp file, fsync, and os.replace."""
    with open(src, "rb") as src_fh:
        _atomic_write_from_stream(src_fh, dst)


class LocalMirrorObjectStore:
    """Local filesystem mirror emulating a bucket-structured object store."""

    BUCKET_NAME = "computeharbor-ckpt"

    def __init__(self, root_dir: Path | str, scheme: str = "gs") -> None:
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self.scheme = scheme

    def _resolve_key(self, key: str) -> tuple[str, Path]:
        clean_key = _validate_object_key(key)
        return clean_key, self.root_dir / Path(clean_key)

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


class HTTPPresignedStore:
    """HTTP presigned or gateway object-store client using stdlib urllib."""

    def __init__(self, base_url: str, timeout_sec: float = 10.0) -> None:
        normalized = base_url.rstrip("/")
        parsed = urllib.parse.urlparse(normalized)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError(f"Invalid HTTP store base_url: {base_url!r}")
        self.base_url = normalized
        self.timeout_sec = timeout_sec

    def _object_url(self, key: str) -> str:
        clean_key = _validate_object_key(key)
        encoded_key = urllib.parse.quote(clean_key, safe="/")
        return f"{self.base_url}/{encoded_key}"

    def put_file(self, local_path: Path, key: str) -> str:
        if not local_path.is_file():
            raise FileNotFoundError(f"Local checkpoint artifact not found: {local_path}")
        url = self._object_url(key)
        payload = local_path.read_bytes()
        req = urllib.request.Request(
            url,
            data=payload,
            method="PUT",
            headers={
                "Content-Type": "application/octet-stream",
                "Content-Length": str(len(payload)),
            },
        )
        with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
            resp.read()
        return url

    def get_file(self, key: str, dest_path: Path) -> None:
        url = self._object_url(key)
        req = urllib.request.Request(url, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                _atomic_write_from_stream(resp, dest_path)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise FileNotFoundError(f"Object not found in store: {key}") from exc
            raise

    def exists(self, key: str) -> bool:
        url = self._object_url(key)
        req = urllib.request.Request(url, method="HEAD")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                return 200 <= int(resp.status) < 300
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return False
            raise


def store_from_uri(
    uri: str,
    mirror_root: Path | None = None,
    timeout_sec: float = 10.0,
) -> CheckpointStore:
    """Instantiate a CheckpointStore implementation from a storage URI or path."""
    if not uri or not uri.strip():
        raise ValueError("Store URI must not be empty")

    parsed = urllib.parse.urlparse(uri)
    scheme = parsed.scheme.lower()

    if scheme in {"http", "https"}:
        return HTTPPresignedStore(base_url=uri, timeout_sec=timeout_sec)

    if scheme == "file":
        path_str = urllib.request.url2pathname(parsed.path)
        if not path_str:
            raise ValueError(f"Invalid file URI: {uri!r}")
        return LocalMirrorObjectStore(Path(path_str))

    if scheme in {"gs", "s3"}:
        if mirror_root is None:
            raise ValueError(
                f"mirror_root is required for {scheme}:// store URIs without a native SDK"
            )
        return LocalMirrorObjectStore(mirror_root, scheme=scheme)

    if scheme == "":
        return LocalMirrorObjectStore(Path(uri))

    raise ValueError(f"Unsupported store URI scheme: {scheme!r}")


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
