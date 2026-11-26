from __future__ import annotations

import argparse
import hashlib
import random
import signal
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from types import FrameType

import numpy as np
import numpy.typing as npt

from computeharbor_worker.checkpoint import CheckpointManager
from computeharbor_worker.models import CheckpointManifest, LoRAConfig, TrainingRunSummary
from computeharbor_worker.storage import (
    CheckpointStore,
    LocalMirrorObjectStore,
    cuda_synchronize_if_available,
)

FloatArray = npt.NDArray[np.float64]
type SignalHandlerType = (
    Callable[[int, FrameType | None], object] | int | signal.Handlers | None
)


@dataclass(frozen=True)
class RunResult:
    summary: TrainingRunSummary
    lora_a: FloatArray
    lora_b: FloatArray
    manifest: CheckpointManifest | None


class PreemptionSignalGuard:
    def __init__(self) -> None:
        self.preempt_requested: bool = False
        self.signal_received: int | None = None
        self._prev_sigterm: SignalHandlerType = signal.SIG_DFL
        self._prev_sigint: SignalHandlerType = signal.SIG_DFL

    def _handle(self, signum: int, _frame: FrameType | None) -> None:
        self.preempt_requested = True
        self.signal_received = signum

    def __enter__(self) -> PreemptionSignalGuard:
        self._prev_sigterm = signal.signal(signal.SIGTERM, self._handle)
        self._prev_sigint = signal.signal(signal.SIGINT, self._handle)
        return self

    def __exit__(self, *_args: object) -> None:
        if self._prev_sigterm is not None:
            signal.signal(signal.SIGTERM, self._prev_sigterm)
        if self._prev_sigint is not None:
            signal.signal(signal.SIGINT, self._prev_sigint)


class LoRATrainer:
    def __init__(
        self,
        *,
        job_id: str,
        config: LoRAConfig,
        checkpoint_dir: Path | str,
        remote_store: CheckpointStore | None = None,
        max_checkpoints: int = 5,
    ) -> None:
        self.job_id = job_id
        self.config = config
        self.checkpoint_mgr = CheckpointManager(
            checkpoint_dir,
            remote_store=remote_store,
            max_checkpoints=max_checkpoints,
            job_id=job_id,
        )

        base_rng = np.random.default_rng(config.base_seed)
        self.frozen_weight: FloatArray = base_rng.standard_normal(
            (config.in_features, config.out_features)
        ) / np.sqrt(config.in_features)
        self.teacher_a: FloatArray = base_rng.standard_normal(
            (config.in_features, config.rank)
        ) * 0.25
        self.teacher_b: FloatArray = base_rng.standard_normal(
            (config.rank, config.out_features)
        ) * 0.25

        self.py_rng = random.Random(config.base_seed + 1)
        self.np_rng = np.random.default_rng(config.base_seed + 2)

        self.lora_a: FloatArray = self.np_rng.standard_normal(
            (config.in_features, config.rank)
        ) * (1.0 / np.sqrt(config.in_features))
        self.lora_b: FloatArray = np.zeros((config.rank, config.out_features), dtype=np.float64)

        self.exp_avg_a: FloatArray = np.zeros_like(self.lora_a)
        self.exp_avg_sq_a: FloatArray = np.zeros_like(self.lora_a)
        self.exp_avg_b: FloatArray = np.zeros_like(self.lora_b)
        self.exp_avg_sq_b: FloatArray = np.zeros_like(self.lora_b)

        self.step: int = 0
        self.epoch: int = 0
        self.loss_history: list[float] = []

    def restore_if_available(self) -> bool:
        if not self.checkpoint_mgr.has_latest():
            return False
        manifest, tensors, py_state, np_state = self.checkpoint_mgr.load_latest()
        self.step = manifest.step
        self.epoch = manifest.epoch
        self.loss_history = list(manifest.loss_history)
        self.lora_a = tensors["lora_a"].copy()
        self.lora_b = tensors["lora_b"].copy()
        self.exp_avg_a = tensors["exp_avg_a"].copy()
        self.exp_avg_sq_a = tensors["exp_avg_sq_a"].copy()
        self.exp_avg_b = tensors["exp_avg_b"].copy()
        self.exp_avg_sq_b = tensors["exp_avg_sq_b"].copy()
        self.py_rng.setstate(py_state)
        self.np_rng.bit_generator.state = np_state
        return True

    def _state_tensors(self) -> dict[str, FloatArray]:
        return {
            "lora_a": self.lora_a,
            "lora_b": self.lora_b,
            "exp_avg_a": self.exp_avg_a,
            "exp_avg_sq_a": self.exp_avg_sq_a,
            "exp_avg_b": self.exp_avg_b,
            "exp_avg_sq_b": self.exp_avg_sq_b,
        }

    def flush_checkpoint(self, *, preempted: bool) -> CheckpointManifest:
        cuda_synchronize_if_available()
        return self.checkpoint_mgr.save(
            job_id=self.job_id,
            step=self.step,
            epoch=self.epoch,
            loss_history=self.loss_history,
            lora_config=self.config,
            tensors=self._state_tensors(),
            py_rng_state=self.py_rng.getstate(),
            np_rng_state=self.np_rng.bit_generator.state,
            preempted=preempted,
        )

    def _train_single_step(self) -> float:
        cfg = self.config
        scale = cfg.alpha / float(cfg.rank)
        jitter = self.py_rng.uniform(0.95, 1.05)

        x: FloatArray = (
            self.np_rng.standard_normal((cfg.batch_size, cfg.in_features)) * jitter
        )
        noise: FloatArray = (
            self.np_rng.standard_normal((cfg.batch_size, cfg.out_features)) * 0.01
        )

        target_delta = (x @ self.teacher_a) @ self.teacher_b
        y_true = (x @ self.frozen_weight) + scale * target_delta + noise

        hidden = x @ self.lora_a
        pred_delta = hidden @ self.lora_b
        y_pred = (x @ self.frozen_weight) + scale * pred_delta

        diff = y_pred - y_true
        loss = float(np.mean(diff * diff))

        grad_output = (2.0 / float(cfg.batch_size * cfg.out_features)) * diff * scale
        grad_b = hidden.T @ grad_output
        grad_a = x.T @ (grad_output @ self.lora_b.T)

        self.step += 1
        if self.step % 10 == 0:
            self.epoch += 1

        b1, b2, eps, lr, wd = (
            cfg.beta1,
            cfg.beta2,
            cfg.eps,
            cfg.learning_rate,
            cfg.weight_decay,
        )
        bc1 = 1.0 - (b1**self.step)
        bc2 = 1.0 - (b2**self.step)

        self.lora_a *= 1.0 - lr * wd
        self.exp_avg_a = b1 * self.exp_avg_a + (1.0 - b1) * grad_a
        self.exp_avg_sq_a = b2 * self.exp_avg_sq_a + (1.0 - b2) * (grad_a * grad_a)
        step_a = (self.exp_avg_a / bc1) / (np.sqrt(self.exp_avg_sq_a / bc2) + eps)
        self.lora_a -= lr * step_a

        self.lora_b *= 1.0 - lr * wd
        self.exp_avg_b = b1 * self.exp_avg_b + (1.0 - b1) * grad_b
        self.exp_avg_sq_b = b2 * self.exp_avg_sq_b + (1.0 - b2) * (grad_b * grad_b)
        step_b = (self.exp_avg_b / bc1) / (np.sqrt(self.exp_avg_sq_b / bc2) + eps)
        self.lora_b -= lr * step_b

        rounded_loss = round(loss, 12)
        self.loss_history.append(rounded_loss)
        return rounded_loss

    def train(
        self,
        *,
        target_steps: int,
        step_delay_sec: float = 0.0,
        simulate_sigterm_at_step: int | None = None,
        ready_file: Path | None = None,
    ) -> RunResult:
        self.restore_if_available()
        start_step = self.step
        manifest: CheckpointManifest | None = None
        preempted = False

        with PreemptionSignalGuard() as guard:
            while self.step < target_steps:
                self._train_single_step()
                if ready_file is not None and not ready_file.exists():
                    ready_file.write_text(str(self.step), encoding="utf-8")

                if simulate_sigterm_at_step is not None and self.step == simulate_sigterm_at_step:
                    guard.preempt_requested = True

                if step_delay_sec > 0.0 and not guard.preempt_requested:
                    deadline = time.perf_counter() + step_delay_sec
                    while time.perf_counter() < deadline:
                        if guard.preempt_requested:
                            break
                        time.sleep(min(0.002, max(0.0, deadline - time.perf_counter())))

                if guard.preempt_requested:
                    preempted = True
                    manifest = self.flush_checkpoint(preempted=True)
                    break

            if not preempted:
                manifest = self.flush_checkpoint(preempted=False)

        a_sha = hashlib.sha256(self.lora_a.tobytes()).hexdigest()
        b_sha = hashlib.sha256(self.lora_b.tobytes()).hexdigest()
        summary = TrainingRunSummary(
            job_id=self.job_id,
            start_step=start_step,
            completed_step=self.step,
            target_steps=target_steps,
            preempted=preempted,
            flush_duration_ms=manifest.flush_duration_ms if manifest else 0.0,
            loss_history=list(self.loss_history),
            adapter_a_sha256=a_sha,
            adapter_b_sha256=b_sha,
            checkpoint_metadata={
                "safetensors_filename": manifest.safetensors_filename if manifest else "",
                "payload_sha256": manifest.payload_sha256 if manifest else "",
            },
        )
        return RunResult(
            summary=summary,
            lora_a=self.lora_a.copy(),
            lora_b=self.lora_b.copy(),
            manifest=manifest,
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="ComputeHarbor Spot LoRA Worker")
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--checkpoint-dir", required=True)
    parser.add_argument("--target-steps", type=int, default=30)
    parser.add_argument("--step-delay-sec", type=float, default=0.0)
    parser.add_argument("--ready-file", default="")
    parser.add_argument("--summary-out", default="")
    parser.add_argument("--remote-store-dir", default="")
    parser.add_argument("--max-checkpoints", type=int, default=5)
    args = parser.parse_args(argv)

    remote_store: CheckpointStore | None = (
        LocalMirrorObjectStore(Path(args.remote_store_dir)) if args.remote_store_dir else None
    )
    trainer = LoRATrainer(
        job_id=args.job_id,
        config=LoRAConfig(),
        checkpoint_dir=Path(args.checkpoint_dir),
        remote_store=remote_store,
        max_checkpoints=args.max_checkpoints,
    )
    ready_path = Path(args.ready_file) if args.ready_file else None
    result = trainer.train(
        target_steps=args.target_steps,
        step_delay_sec=args.step_delay_sec,
        ready_file=ready_path,
    )
    output_json = result.summary.model_dump_json(indent=2)
    if args.summary_out:
        Path(args.summary_out).write_text(output_json, encoding="utf-8")
    else:
        sys.stdout.write(output_json + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
