from __future__ import annotations

import importlib
import random
import sys
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pytest
from computeharbor_worker.checkpoint import CheckpointManager
from computeharbor_worker.models import CheckpointManifest, LoRAConfig
from computeharbor_worker.storage import (
    CheckpointStore,
    HTTPPresignedStore,
    LocalMirrorObjectStore,
    cuda_synchronize_if_available,
    store_from_uri,
)
from computeharbor_worker.trainer import LoRATrainer, main


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


@contextmanager
def _in_memory_object_http_server() -> Iterator[str]:
    objects: dict[str, bytes] = {}
    lock = threading.Lock()

    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            return

        def do_PUT(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            with lock:
                objects[self.path] = body
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self) -> None:
            with lock:
                payload = objects.get(self.path)
            if payload is None:
                self.send_response(404)
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_HEAD(self) -> None:
            with lock:
                found = self.path in objects
            self.send_response(200 if found else 404)
            self.end_headers()

    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        _, port = server.server_address[:2]
        yield f"http://127.0.0.1:{port}/presigned"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)


def test_http_presigned_store_put_get_exists(tmp_path: Path) -> None:
    with _in_memory_object_http_server() as base_url:
        store = HTTPPresignedStore(base_url)
        assert isinstance(store, CheckpointStore)

        src = tmp_path / "weights.safetensors"
        payload = b"presigned-checkpoint-payload-v1"
        src.write_bytes(payload)

        key = "job-http-1/step-000010.safetensors"
        assert not store.exists(key)

        uri = store.put_file(src, key)
        assert uri == f"{base_url}/{key}"
        assert store.exists(key)

        dest = tmp_path / "downloaded" / "weights.safetensors"
        store.get_file(key, dest)
        assert dest.read_bytes() == payload
        assert list(dest.parent.glob("*.tmp")) == []


def test_http_presigned_store_validation_and_errors(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Invalid HTTP store base_url"):
        HTTPPresignedStore("ftp://example.com/bucket")

    with _in_memory_object_http_server() as base_url:
        store = HTTPPresignedStore(base_url)
        src = tmp_path / "data.bin"
        src.write_bytes(b"abc")

        with pytest.raises(ValueError, match="Invalid object key"):
            store.put_file(src, "../escape.bin")

        with pytest.raises(ValueError, match="empty"):
            store.exists("/")

        with pytest.raises(FileNotFoundError):
            store.put_file(tmp_path / "missing.bin", "job/step.bin")

        with pytest.raises(FileNotFoundError):
            store.get_file("missing/key.bin", tmp_path / "out.bin")


def test_store_from_uri_routing(tmp_path: Path) -> None:
    http_store = store_from_uri("http://127.0.0.1:9000/bucket")
    assert isinstance(http_store, HTTPPresignedStore)

    https_store = store_from_uri("https://storage.example.com/presigned/")
    assert isinstance(https_store, HTTPPresignedStore)
    assert https_store.base_url == "https://storage.example.com/presigned"

    file_dir = tmp_path / "file_mirror"
    file_store = store_from_uri(file_dir.as_uri())
    assert isinstance(file_store, LocalMirrorObjectStore)
    assert file_store.root_dir == file_dir

    plain_dir = tmp_path / "plain_mirror"
    plain_store = store_from_uri(str(plain_dir))
    assert isinstance(plain_store, LocalMirrorObjectStore)
    assert plain_store.root_dir == plain_dir

    gs_mirror = tmp_path / "gs_mirror"
    gs_store = store_from_uri("gs://computeharbor-ckpt", mirror_root=gs_mirror)
    assert isinstance(gs_store, LocalMirrorObjectStore)
    assert gs_store.root_dir == gs_mirror
    assert gs_store.scheme == "gs"

    s3_mirror = tmp_path / "s3_mirror"
    s3_store = store_from_uri("s3://computeharbor-ckpt", mirror_root=s3_mirror)
    assert isinstance(s3_store, LocalMirrorObjectStore)
    assert s3_store.root_dir == s3_mirror
    assert s3_store.scheme == "s3"

    with pytest.raises(ValueError, match="mirror_root is required"):
        store_from_uri("gs://computeharbor-ckpt")

    with pytest.raises(ValueError, match="mirror_root is required"):
        store_from_uri("s3://computeharbor-ckpt")

    with pytest.raises(ValueError, match="empty"):
        store_from_uri("")

    with pytest.raises(ValueError, match="Unsupported store URI scheme"):
        store_from_uri("ftp://example.com/ckpt")


def test_fresh_node_restores_from_remote_store_on_empty_local_dir(tmp_path: Path) -> None:
    cfg = LoRAConfig(in_features=32, out_features=16, rank=4, base_seed=777)

    baseline_trainer = LoRATrainer(
        job_id="job-remote-restore-baseline",
        config=cfg,
        checkpoint_dir=tmp_path / "baseline_ckpt",
    )
    baseline = baseline_trainer.train(target_steps=15)
    assert len(baseline.summary.loss_history) == 15

    mirror_dir = tmp_path / "mirror"
    remote_store = LocalMirrorObjectStore(mirror_dir)

    node1_ckpt = tmp_path / "node1_ckpt"
    node1 = LoRATrainer(
        job_id="job-remote-restore",
        config=cfg,
        checkpoint_dir=node1_ckpt,
        remote_store=remote_store,
    )
    node1_res = node1.train(target_steps=8)
    assert node1_res.summary.completed_step == 8
    assert remote_store.exists("job-remote-restore/latest.json")

    node2_ckpt = tmp_path / "node2_ckpt"
    node2 = LoRATrainer(
        job_id="job-remote-restore",
        config=cfg,
        checkpoint_dir=node2_ckpt,
        remote_store=remote_store,
    )
    assert node2.restore_if_available() is True
    assert node2.step == 8

    node2_res = node2.train(target_steps=15)
    assert node2_res.summary.completed_step == 15
    assert node2_res.summary.loss_history == baseline.summary.loss_history
    assert node2_res.summary.adapter_a_sha256 == baseline.summary.adapter_a_sha256
    assert node2_res.summary.adapter_b_sha256 == baseline.summary.adapter_b_sha256
    np.testing.assert_array_equal(node2_res.lora_a, baseline.lora_a)
    np.testing.assert_array_equal(node2_res.lora_b, baseline.lora_b)


def test_remote_store_isolates_concurrent_jobs(tmp_path: Path) -> None:
    cfg_alpha = LoRAConfig(in_features=16, out_features=8, rank=2, base_seed=101)
    cfg_beta = LoRAConfig(in_features=16, out_features=8, rank=2, base_seed=202)
    mirror_dir = tmp_path / "mirror"
    remote_store = LocalMirrorObjectStore(mirror_dir)

    alpha_node1 = LoRATrainer(
        job_id="job-alpha",
        config=cfg_alpha,
        checkpoint_dir=tmp_path / "alpha_node1",
        remote_store=remote_store,
    )
    alpha_res = alpha_node1.train(target_steps=6)
    assert alpha_res.summary.completed_step == 6

    beta_node1 = LoRATrainer(
        job_id="job-beta",
        config=cfg_beta,
        checkpoint_dir=tmp_path / "beta_node1",
        remote_store=remote_store,
    )
    beta_res = beta_node1.train(target_steps=10)
    assert beta_res.summary.completed_step == 10

    assert remote_store.exists("job-alpha/latest.json")
    assert remote_store.exists("job-beta/latest.json")
    assert not remote_store.exists("latest.json")

    alpha_node2 = LoRATrainer(
        job_id="job-alpha",
        config=cfg_alpha,
        checkpoint_dir=tmp_path / "alpha_node2_empty",
        remote_store=remote_store,
    )
    assert alpha_node2.restore_if_available() is True
    assert alpha_node2.step == 6
    np.testing.assert_array_equal(alpha_node2.lora_a, alpha_res.lora_a)
    np.testing.assert_array_equal(alpha_node2.lora_b, alpha_res.lora_b)

    beta_node2 = LoRATrainer(
        job_id="job-beta",
        config=cfg_beta,
        checkpoint_dir=tmp_path / "beta_node2_empty",
        remote_store=remote_store,
    )
    assert beta_node2.restore_if_available() is True
    assert beta_node2.step == 10
    np.testing.assert_array_equal(beta_node2.lora_a, beta_res.lora_a)
    np.testing.assert_array_equal(beta_node2.lora_b, beta_res.lora_b)
