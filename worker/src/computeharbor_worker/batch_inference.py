from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
from pydantic import BaseModel, ConfigDict, Field

from computeharbor_worker.checkpoint import (
    _atomic_write_bytes,
    _decode_json_safe,
    _encode_json_safe,
)
from computeharbor_worker.trainer import PreemptionSignalGuard


class BatchCursorManifest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    job_id: str = Field(min_length=1)
    processed_count: int = Field(ge=0)
    total_records: int = Field(ge=0)
    output_sha256: str = Field(min_length=64, max_length=64)
    preempted: bool = False
    numpy_rng_state_json: str
    flush_duration_ms: float = Field(ge=0.0)
    elapsed_ms: float = Field(default=0.0, ge=0.0)
    records_per_sec: float = Field(default=0.0, ge=0.0)
    estimated_cost_per_million_records_usd: float = Field(default=0.0, ge=0.0)


BatchRunSummary = BatchCursorManifest


def _normalize_prompt(item: str | Mapping[str, Any]) -> str:
    if isinstance(item, str):
        return item
    if "prompt" in item:
        return str(item["prompt"])
    if "text" in item:
        return str(item["text"])
    return json.dumps(dict(item), sort_keys=True, separators=(",", ":"))


class BatchInferenceRunner:
    """Deterministic, checkpointed batch inference worker with atomic SIGTERM recovery."""

    def __init__(
        self,
        job_id: str,
        checkpoint_dir: Path | str,
        seed: int = 42,
        batch_size: int = 8,
        spot_hourly_usd: float = 0.24,
    ) -> None:
        if batch_size <= 0:
            raise ValueError(f"batch_size must be positive, got {batch_size}")
        if spot_hourly_usd < 0.0:
            raise ValueError(f"spot_hourly_usd must be non-negative, got {spot_hourly_usd}")
        self.job_id = job_id
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.seed = seed
        self.batch_size = batch_size
        self.spot_hourly_usd = spot_hourly_usd
        self.np_rng = np.random.default_rng(seed)

        self.output_path = self.checkpoint_dir / "output.jsonl"
        self.manifest_path = self.checkpoint_dir / "batch_cursor.manifest.json"
        self._lines: list[str] = []
        self._reconcile_stale_temporaries()

    def _reconcile_stale_temporaries(self) -> int:
        removed = 0
        for stale in self.checkpoint_dir.glob("*.tmp"):
            stale.unlink(missing_ok=True)
            removed += 1
        return removed

    def has_checkpoint(self) -> bool:
        return self.manifest_path.is_file() and self.output_path.is_file()

    def restore_if_available(self) -> BatchCursorManifest | None:
        self._reconcile_stale_temporaries()
        if not self.manifest_path.is_file():
            self._lines = []
            return None
        if not self.output_path.is_file():
            raise FileNotFoundError(f"Missing output JSONL file at {self.output_path}")

        manifest = BatchCursorManifest.model_validate_json(
            self.manifest_path.read_text(encoding="utf-8")
        )
        raw_bytes = self.output_path.read_bytes()
        actual_sha256 = hashlib.sha256(raw_bytes).hexdigest()
        if actual_sha256 != manifest.output_sha256:
            raise ValueError(
                f"Output JSONL SHA256 mismatch: expected {manifest.output_sha256}, "
                f"got {actual_sha256}"
            )

        decoded = raw_bytes.decode("utf-8")
        lines = decoded.splitlines(keepends=True)
        if len(lines) != manifest.processed_count:
            raise ValueError(
                f"Output JSONL line count mismatch: expected {manifest.processed_count}, "
                f"got {len(lines)}"
            )

        np_state = _decode_json_safe(json.loads(manifest.numpy_rng_state_json))
        if not isinstance(np_state, dict):
            raise ValueError("Corrupted numpy RNG state in BatchCursorManifest")
        self.np_rng.bit_generator.state = np_state
        self._lines = lines
        return manifest

    def _infer_record(self, index: int, prompt: str) -> str:
        rng_val = float(self.np_rng.random())
        prompt_digest = hashlib.sha256(f"{index}:{prompt}".encode()).hexdigest()
        score = round(0.5 * rng_val + 0.5 * (int(prompt_digest[:8], 16) / 0xFFFFFFFF), 8)
        token_digest = hashlib.sha256(
            f"{prompt_digest}:{rng_val:.12f}".encode()
        ).hexdigest()[:16]
        record = {
            "index": index,
            "prompt": prompt,
            "score": score,
            "token_digest": token_digest,
        }
        return json.dumps(record, separators=(",", ":")) + "\n"

    def _flush(
        self,
        *,
        total_records: int,
        preempted: bool,
        elapsed_ms: float = 0.0,
        records_in_run: int = 0,
    ) -> BatchCursorManifest:
        t0 = time.perf_counter()
        output_bytes = "".join(self._lines).encode("utf-8")
        _atomic_write_bytes(self.output_path, output_bytes)
        output_sha256 = hashlib.sha256(output_bytes).hexdigest()
        flush_ms = round((time.perf_counter() - t0) * 1000.0, 4)

        elapsed_ms_rounded = round(max(0.0, elapsed_ms), 4)
        elapsed_sec = elapsed_ms_rounded / 1000.0
        if records_in_run > 0 and elapsed_sec > 0.0:
            records_per_sec = round(records_in_run / elapsed_sec, 4)
            cost_per_sec = self.spot_hourly_usd / 3600.0
            estimated_cost_per_million = round(
                (1_000_000.0 / records_per_sec) * cost_per_sec,
                6,
            )
        else:
            records_per_sec = 0.0
            estimated_cost_per_million = 0.0

        np_state_json = json.dumps(
            _encode_json_safe(self.np_rng.bit_generator.state),
            sort_keys=True,
        )
        manifest = BatchCursorManifest(
            job_id=self.job_id,
            processed_count=len(self._lines),
            total_records=total_records,
            output_sha256=output_sha256,
            preempted=preempted,
            numpy_rng_state_json=np_state_json,
            flush_duration_ms=flush_ms,
            elapsed_ms=elapsed_ms_rounded,
            records_per_sec=records_per_sec,
            estimated_cost_per_million_records_usd=estimated_cost_per_million,
        )
        _atomic_write_bytes(
            self.manifest_path,
            manifest.model_dump_json(indent=2).encode("utf-8"),
        )
        return manifest

    def run(
        self,
        prompts: Sequence[str | Mapping[str, Any]],
        *,
        record_delay_sec: float = 0.0,
        simulate_sigterm_at_index: int | None = None,
        ready_file: Path | None = None,
    ) -> BatchCursorManifest:
        run_start = time.perf_counter()
        normalized_prompts = [_normalize_prompt(p) for p in prompts]
        total_records = len(normalized_prompts)
        self.restore_if_available()

        start_idx = len(self._lines)
        preempted = False
        with PreemptionSignalGuard() as guard:
            for batch_start in range(start_idx, total_records, self.batch_size):
                batch_end = min(batch_start + self.batch_size, total_records)
                for idx in range(batch_start, batch_end):
                    self._lines.append(self._infer_record(idx, normalized_prompts[idx]))

                    if ready_file is not None and not ready_file.exists():
                        ready_file.write_text(str(len(self._lines)), encoding="utf-8")

                    if (
                        simulate_sigterm_at_index is not None
                        and len(self._lines) == simulate_sigterm_at_index
                    ):
                        guard.preempt_requested = True

                    if record_delay_sec > 0.0 and not guard.preempt_requested:
                        deadline = time.perf_counter() + record_delay_sec
                        while time.perf_counter() < deadline:
                            if guard.preempt_requested:
                                break
                            time.sleep(min(0.002, max(0.0, deadline - time.perf_counter())))

                    if guard.preempt_requested:
                        preempted = True
                        return self._flush(
                            total_records=total_records,
                            preempted=True,
                            elapsed_ms=(time.perf_counter() - run_start) * 1000.0,
                            records_in_run=len(self._lines) - start_idx,
                        )

                if guard.preempt_requested:
                    preempted = True
                    return self._flush(
                        total_records=total_records,
                        preempted=True,
                        elapsed_ms=(time.perf_counter() - run_start) * 1000.0,
                        records_in_run=len(self._lines) - start_idx,
                    )

        return self._flush(
            total_records=total_records,
            preempted=preempted,
            elapsed_ms=(time.perf_counter() - run_start) * 1000.0,
            records_in_run=len(self._lines) - start_idx,
        )


def _load_prompts(prompts_file: str, num_prompts: int) -> list[str | Mapping[str, Any]]:
    if not prompts_file:
        return [f"prompt-{i:04d}" for i in range(num_prompts)]
    raw_text = Path(prompts_file).read_text(encoding="utf-8")
    parsed = json.loads(raw_text)
    if not isinstance(parsed, list):
        raise ValueError(f"Expected JSON array in {prompts_file}")
    prompts: list[str | Mapping[str, Any]] = []
    for entry in parsed:
        if isinstance(entry, str | dict):
            prompts.append(entry)
        else:
            raise ValueError(f"Unsupported prompt entry type: {type(entry).__name__}")
    return prompts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="ComputeHarbor Deterministic Batch Inference")
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--checkpoint-dir", required=True)
    parser.add_argument("--prompts-file", default="")
    parser.add_argument("--num-prompts", type=int, default=50)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--spot-hourly-usd", type=float, default=0.24)
    parser.add_argument("--record-delay-sec", type=float, default=0.0)
    parser.add_argument("--ready-file", default="")
    parser.add_argument("--manifest-out", default="")
    args = parser.parse_args(argv)

    prompts = _load_prompts(args.prompts_file, args.num_prompts)
    runner = BatchInferenceRunner(
        job_id=args.job_id,
        checkpoint_dir=Path(args.checkpoint_dir),
        seed=args.seed,
        batch_size=args.batch_size,
        spot_hourly_usd=args.spot_hourly_usd,
    )
    ready_path = Path(args.ready_file) if args.ready_file else None
    manifest = runner.run(
        prompts,
        record_delay_sec=args.record_delay_sec,
        ready_file=ready_path,
    )
    output_json = manifest.model_dump_json(indent=2)
    if args.manifest_out:
        Path(args.manifest_out).write_text(output_json, encoding="utf-8")
    else:
        sys.stdout.write(output_json + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
