from __future__ import annotations

import random
from pathlib import Path

import numpy as np
import pytest
from computeharbor_worker.checkpoint import CheckpointManager
from computeharbor_worker.models import LoRAConfig
from computeharbor_worker.trainer import LoRATrainer


def test_atomic_checkpoint_reconciles_stale_tmp_files(tmp_path: Path) -> None:
    stale_tmp = tmp_path / ".step-000001.safetensors.9999.tmp"
    stale_tmp.write_bytes(b"partial-eviction-write")

    mgr = CheckpointManager(tmp_path)
    assert not stale_tmp.exists()
    assert not mgr.has_latest()


def test_checkpoint_tamper_detection_raises(tmp_path: Path) -> None:
    trainer = LoRATrainer(
        job_id="job-tamper",
        config=LoRAConfig(),
        checkpoint_dir=tmp_path,
    )
    res = trainer.train(target_steps=5)
    assert res.manifest is not None

    st_file = tmp_path / res.manifest.safetensors_filename
    raw = bytearray(st_file.read_bytes())
    raw[-1] ^= 0xFF
    st_file.write_bytes(bytes(raw))

    resumed = LoRATrainer(
        job_id="job-tamper",
        config=LoRAConfig(),
        checkpoint_dir=tmp_path,
    )
    with pytest.raises(ValueError, match="SHA256 mismatch"):
        resumed.restore_if_available()


def test_in_process_three_preemptions_bit_for_bit_parity(tmp_path: Path) -> None:
    cfg = LoRAConfig(in_features=48, out_features=32, rank=8, base_seed=2026)

    baseline_dir = tmp_path / "baseline"
    baseline_trainer = LoRATrainer(
        job_id="job-baseline",
        config=cfg,
        checkpoint_dir=baseline_dir,
    )
    baseline = baseline_trainer.train(target_steps=24)
    assert len(baseline.summary.loss_history) == 24
    assert baseline.summary.loss_history[-1] < baseline.summary.loss_history[0]

    preempt_dir = tmp_path / "preempted"
    for stop_step in (6, 13, 19):
        worker = LoRATrainer(
            job_id="job-preempt",
            config=cfg,
            checkpoint_dir=preempt_dir,
        )
        partial = worker.train(target_steps=24, simulate_sigterm_at_step=stop_step)
        assert partial.summary.preempted is True
        assert partial.summary.completed_step == stop_step
        assert partial.summary.flush_duration_ms < 30_000.0

    final_worker = LoRATrainer(
        job_id="job-preempt",
        config=cfg,
        checkpoint_dir=preempt_dir,
    )
    recovered = final_worker.train(target_steps=24)
    assert recovered.summary.preempted is False
    assert recovered.summary.completed_step == 24
    assert recovered.summary.loss_history == baseline.summary.loss_history
    assert recovered.summary.adapter_a_sha256 == baseline.summary.adapter_a_sha256
    assert recovered.summary.adapter_b_sha256 == baseline.summary.adapter_b_sha256
    np.testing.assert_array_equal(recovered.lora_a, baseline.lora_a)
    np.testing.assert_array_equal(recovered.lora_b, baseline.lora_b)


def test_checkpoint_retention_prunes_oldest_and_restores_latest(tmp_path: Path) -> None:
    mgr = CheckpointManager(tmp_path, max_checkpoints=2)
    cfg = LoRAConfig()
    py_rng = random.Random(42)
    np_rng = np.random.default_rng(42)

    for step in range(1, 6):
        tensors = {"lora_a": np.full((2, 2), float(step), dtype=np.float64)}
        mgr.save(
            job_id="job-retention",
            step=step,
            epoch=0,
            loss_history=[1.0 / step],
            lora_config=cfg,
            tensors=tensors,
            py_rng_state=py_rng.getstate(),
            np_rng_state=np_rng.bit_generator.state,
        )

    files = sorted(p.name for p in tmp_path.iterdir())
    assert files == [
        "latest.json",
        "step-000004.manifest.json",
        "step-000004.safetensors",
        "step-000005.manifest.json",
        "step-000005.safetensors",
    ]

    manifest, tensors, _, _ = mgr.load_latest()
    assert manifest.step == 5
    np.testing.assert_array_equal(tensors["lora_a"], np.full((2, 2), 5.0, dtype=np.float64))


def test_checkpoint_retention_unlimited_when_zero(tmp_path: Path) -> None:
    mgr = CheckpointManager(tmp_path, max_checkpoints=0)
    cfg = LoRAConfig()
    py_rng = random.Random(7)
    np_rng = np.random.default_rng(7)

    for step in range(1, 6):
        mgr.save(
            job_id="job-unlimited",
            step=step,
            epoch=0,
            loss_history=[0.5],
            lora_config=cfg,
            tensors={"lora_a": np.ones((2, 2), dtype=np.float64)},
            py_rng_state=py_rng.getstate(),
            np_rng_state=np_rng.bit_generator.state,
        )

    assert len(list(tmp_path.glob("step-*.manifest.json"))) == 5
    assert mgr.prune_old_checkpoints() == 0


def test_checkpoint_manager_negative_max_checkpoints_raises(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="max_checkpoints"):
        CheckpointManager(tmp_path, max_checkpoints=-1)
