from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class LoRAConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    in_features: int = Field(default=64, gt=0)
    out_features: int = Field(default=64, gt=0)
    rank: int = Field(default=8, gt=0)
    alpha: float = Field(default=16.0, gt=0.0)
    learning_rate: float = Field(default=5e-3, gt=0.0)
    weight_decay: float = Field(default=1e-2, ge=0.0)
    beta1: float = Field(default=0.9, gt=0.0, lt=1.0)
    beta2: float = Field(default=0.999, gt=0.0, lt=1.0)
    eps: float = Field(default=1e-8, gt=0.0)
    batch_size: int = Field(default=16, gt=0)
    base_seed: int = Field(default=42, ge=0)


class RNGStatePayload(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    python_random_state_json: str
    numpy_bit_generator_state_json: str


class CheckpointManifest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    job_id: str = Field(min_length=1)
    step: int = Field(ge=0)
    epoch: int = Field(ge=0)
    loss_history: list[float]
    safetensors_filename: str = Field(min_length=1)
    payload_sha256: str = Field(min_length=64, max_length=64)
    flush_duration_ms: float = Field(ge=0.0)
    preempted: bool = False
    remote_uri: str | None = None
    remote_upload_ms: float = Field(default=0.0, ge=0.0)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    lora_config: LoRAConfig
    rng_state: RNGStatePayload


class TrainingRunSummary(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    job_id: str
    start_step: int
    completed_step: int
    target_steps: int
    preempted: bool
    flush_duration_ms: float
    loss_history: list[float]
    adapter_a_sha256: str
    adapter_b_sha256: str
    checkpoint_metadata: dict[str, Any] = Field(default_factory=dict)
