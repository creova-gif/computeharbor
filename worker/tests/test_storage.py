from __future__ import annotations

import importlib
import random
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pytest
from computeharbor_worker.checkpoint import CheckpointManager
from computeharbor_worker.models import CheckpointManifest, LoRAConfig
from computeharbor_worker.storage import (
    CheckpointStore,
    LocalMirrorObjectStore,
    cuda_synchronize_if_available,
)
from computeharbor_worker.trainer import main


def test_local_mirror_object_store_put_get_exists(tmp_path: Path) -> None:
    store_dir = tmp_path / "mirror"
    store = LocalMirrorObjectStore(store_dir)
    assert isinstance(store, CheckpointStore)

    src = tmp_path / "artifact.bin"
    payload = b"computeharbor-checkpoint-bytes-v1"
    src.write_bytes(payload)

    key = "job-123/step-000005.safetensors"
    assert not store.exists(key)

    uri = store.put_file(src, key)
    assert uri == f"gs://computeharbor-ckpt/{key}"
    assert store.exists(key)
    assert (store_dir / key).read_bytes() == payload
    assert list((store_dir / "job-123").glob("*.tmp")) == []

    dest = tmp_path / "restored" / "artifact.bin"
    store.get_file(key, dest)
    assert dest.read_bytes() == payload


def test_local_mirror_object_store_rejects_invalid_keys_and_missing(tmp_path: Path) -> None:
    store = LocalMirrorObjectStore(tmp_path / "mirror", scheme="s3")
    src = tmp_path / "data.bin"
    src.write_bytes(b"abc")

    assert store.put_file(src, "ok/key.bin") == "s3://computeharbor-ckpt/ok/key.bin"

    with pytest.raises(ValueError, match="Invalid object key"):
        store.put_file(src, "../escape.bin")

    with pytest.raises(ValueError, match="empty"):
        store.exists("/")

    with pytest.raises(FileNotFoundError):
        store.get_file("missing/key.bin", tmp_path / "out.bin")


def test_checkpoint_manager_remote_replication(tmp_path: Path) -> None:
    local_dir = tmp_path / "local"
    remote_dir = tmp_path / "remote"
    store = LocalMirrorObjectStore(remote_dir)
    mgr = CheckpointManager(local_dir, remote_store=store)

    py_rng = random.Random(42)
    np_rng = np.random.default_rng(42)
    tensors = {"lora_a": np.ones((4, 2), dtype=np.float64)}

    manifest = mgr.save(
        job_id="job-remote-1",
        step=7,
        epoch=1,
        loss_history=[0.5, 0.25],
        lora_config=LoRAConfig(in_features=4, out_features=4, rank=2),
        tensors=tensors,
        py_rng_state=py_rng.getstate(),
        np_rng_state=np_rng.bit_generator.state,
    )

    st_key = "job-remote-1/step-000007.safetensors"
    manifest_key = "job-remote-1/step-000007.manifest.json"
    assert manifest.remote_uri == f"gs://computeharbor-ckpt/{st_key}"
    assert manifest.remote_upload_ms >= 0.0
    assert store.exists(st_key)
    assert store.exists(manifest_key)

    replicated_manifest = CheckpointManifest.model_validate_json(
        (remote_dir / manifest_key).read_text(encoding="utf-8")
    )
    assert replicated_manifest.remote_uri == manifest.remote_uri
    assert replicated_manifest.payload_sha256 == manifest.payload_sha256


def test_trainer_cli_remote_store_dir(tmp_path: Path) -> None:
    ckpt_dir = tmp_path / "ckpt"
    remote_dir = tmp_path / "remote"
    summary_path = tmp_path / "summary.json"

    rc = main(
        [
            "--job-id",
            "job-cli-remote",
            "--checkpoint-dir",
            str(ckpt_dir),
            "--remote-store-dir",
            str(remote_dir),
            "--target-steps",
            "4",
            "--summary-out",
            str(summary_path),
        ]
    )
    assert rc == 0
    assert (remote_dir / "job-cli-remote" / "step-000004.safetensors").is_file()
    assert (remote_dir / "job-cli-remote" / "step-000004.manifest.json").is_file()


def test_cuda_synchronize_if_available_cpu_and_mocked_cuda(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Simulate torch not installed
    monkeypatch.delitem(sys.modules, "torch", raising=False)
    real_import_module = importlib.import_module

    def _raise_for_torch(name: str, package: str | None = None) -> Any:
        if name == "torch":
            raise ImportError("No module named 'torch'")
        return real_import_module(name, package=package)

    monkeypatch.setattr(importlib, "import_module", _raise_for_torch)
    assert cuda_synchronize_if_available() is False
    monkeypatch.undo()

    # Simulate torch installed without CUDA
    fake_torch_no_cuda = SimpleNamespace(
        cuda=SimpleNamespace(is_available=lambda: False, synchronize=lambda: None)
    )
    monkeypatch.setitem(sys.modules, "torch", fake_torch_no_cuda)
    assert cuda_synchronize_if_available() is False

    # Simulate torch installed with CUDA available
    sync_calls = 0

    def _sync() -> None:
        nonlocal sync_calls
        sync_calls += 1

    fake_torch_cuda = SimpleNamespace(
        cuda=SimpleNamespace(is_available=lambda: True, synchronize=_sync)
    )
    monkeypatch.setitem(sys.modules, "torch", fake_torch_cuda)
    assert cuda_synchronize_if_available() is True
    assert sync_calls == 1
