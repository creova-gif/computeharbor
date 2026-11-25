from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt
from safetensors.numpy import load_file, save_file

from computeharbor_worker.models import CheckpointManifest, LoRAConfig, RNGStatePayload
from computeharbor_worker.storage import CheckpointStore

FloatArray = npt.NDArray[np.float64]


def _encode_json_safe(obj: Any) -> Any:
    if isinstance(obj, np.ndarray):
        return {"__ndarray__": obj.tolist(), "dtype": str(obj.dtype)}
    if isinstance(obj, np.generic):
        return obj.item()
    if isinstance(obj, tuple):
        return {"__tuple__": [_encode_json_safe(x) for x in obj]}
    if isinstance(obj, list):
        return [_encode_json_safe(x) for x in obj]
    if isinstance(obj, dict):
        return {str(k): _encode_json_safe(v) for k, v in obj.items()}
    return obj


def _decode_json_safe(obj: Any) -> Any:
    if isinstance(obj, dict):
        if "__ndarray__" in obj:
            return np.array(obj["__ndarray__"], dtype=np.dtype(obj["dtype"]))
        if "__tuple__" in obj:
            return tuple(_decode_json_safe(x) for x in obj["__tuple__"])
        return {k: _decode_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_decode_json_safe(x) for x in obj]
    return obj


def serialize_rng_state(
    py_state: tuple[Any, ...],
    np_state: Mapping[str, Any],
) -> RNGStatePayload:
    return RNGStatePayload(
        python_random_state_json=json.dumps(_encode_json_safe(py_state), sort_keys=True),
        numpy_bit_generator_state_json=json.dumps(_encode_json_safe(np_state), sort_keys=True),
    )


def deserialize_rng_state(
    payload: RNGStatePayload,
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    py_raw = _decode_json_safe(json.loads(payload.python_random_state_json))
    np_raw = _decode_json_safe(json.loads(payload.numpy_bit_generator_state_json))
    if not isinstance(py_raw, tuple) or not isinstance(np_raw, dict):
        raise ValueError("Corrupted RNG state payload")
    return py_raw, np_raw


def _atomic_write_bytes(target: Path, data: bytes) -> None:
    tmp_path = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    with open(tmp_path, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp_path, target)


class CheckpointManager:
    def __init__(
        self,
        checkpoint_dir: Path | str,
        remote_store: CheckpointStore | None = None,
    ) -> None:
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.remote_store = remote_store
        self.reconcile_stale_temporaries()

    def reconcile_stale_temporaries(self) -> int:
        removed = 0
        for stale in self.checkpoint_dir.glob("*.tmp"):
            stale.unlink(missing_ok=True)
            removed += 1
        return removed

    def save(
        self,
        *,
        job_id: str,
        step: int,
        epoch: int,
        loss_history: list[float],
        lora_config: LoRAConfig,
        tensors: dict[str, FloatArray],
        py_rng_state: tuple[Any, ...],
        np_rng_state: Mapping[str, Any],
        preempted: bool = False,
    ) -> CheckpointManifest:
        t0 = time.perf_counter()
        st_filename = f"step-{step:06d}.safetensors"
        st_target = self.checkpoint_dir / st_filename
        st_tmp = self.checkpoint_dir / f".{st_filename}.{os.getpid()}.tmp"

        save_file(tensors, str(st_tmp))
        with open(st_tmp, "rb") as fh:
            os.fsync(fh.fileno())
            payload_bytes = fh.read()
        os.replace(st_tmp, st_target)

        digest = hashlib.sha256(payload_bytes).hexdigest()
        flush_ms = (time.perf_counter() - t0) * 1000.0

        remote_uri: str | None = None
        remote_upload_ms = 0.0
        if self.remote_store is not None:
            upload_t0 = time.perf_counter()
            remote_uri = self.remote_store.put_file(st_target, f"{job_id}/{st_filename}")
            remote_upload_ms = round((time.perf_counter() - upload_t0) * 1000.0, 4)

        manifest = CheckpointManifest(
            job_id=job_id,
            step=step,
            epoch=epoch,
            loss_history=list(loss_history),
            safetensors_filename=st_filename,
            payload_sha256=digest,
            flush_duration_ms=round(flush_ms, 4),
            preempted=preempted,
            remote_uri=remote_uri,
            remote_upload_ms=remote_upload_ms,
            lora_config=lora_config,
            rng_state=serialize_rng_state(py_rng_state, np_rng_state),
        )

        manifest_bytes = manifest.model_dump_json(indent=2).encode("utf-8")
        step_manifest_path = self.checkpoint_dir / f"step-{step:06d}.manifest.json"
        latest_manifest_path = self.checkpoint_dir / "latest.json"

        _atomic_write_bytes(step_manifest_path, manifest_bytes)
        _atomic_write_bytes(latest_manifest_path, manifest_bytes)
        if self.remote_store is not None:
            self.remote_store.put_file(step_manifest_path, f"{job_id}/{step_manifest_path.name}")
        return manifest

    def has_latest(self) -> bool:
        return (self.checkpoint_dir / "latest.json").is_file()

    def load_latest(
        self,
    ) -> tuple[CheckpointManifest, dict[str, FloatArray], tuple[Any, ...], dict[str, Any]]:
        self.reconcile_stale_temporaries()
        latest_path = self.checkpoint_dir / "latest.json"
        if not latest_path.is_file():
            raise FileNotFoundError(f"No checkpoint manifest found at {latest_path}")

        manifest = CheckpointManifest.model_validate_json(latest_path.read_text(encoding="utf-8"))
        st_path = self.checkpoint_dir / manifest.safetensors_filename
        if not st_path.is_file():
            raise FileNotFoundError(f"Missing Safetensors file {st_path}")

        actual_sha256 = hashlib.sha256(st_path.read_bytes()).hexdigest()
        if actual_sha256 != manifest.payload_sha256:
            raise ValueError(
                f"Safetensors SHA256 mismatch: expected {manifest.payload_sha256}, "
                f"got {actual_sha256}"
            )

        raw_tensors = load_file(str(st_path))
        tensors: dict[str, FloatArray] = {
            k: np.asarray(v, dtype=np.float64) for k, v in raw_tensors.items()
        }
        py_state, np_state = deserialize_rng_state(manifest.rng_state)
        return manifest, tensors, py_state, np_state
