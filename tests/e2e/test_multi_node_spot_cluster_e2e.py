from __future__ import annotations

import socket
import subprocess
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from computeharbor_worker.client import ControlPlaneClient
from computeharbor_worker.models import LoRAConfig
from computeharbor_worker.preemption import MetadataPreemptionWatcher
from computeharbor_worker.trainer import LoRATrainer


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_healthz(base_url: str, timeout_sec: float = 10.0) -> None:
    deadline = time.perf_counter() + timeout_sec
    while time.perf_counter() < deadline:
        try:
            with urllib.request.urlopen(f"{base_url}/healthz", timeout=0.5) as resp:
                if resp.status == 200:
                    return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"Control plane did not become healthy at {base_url}")


def test_multi_node_barrier_and_gcp_metadata_eviction_e2e(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    binary = tmp_path / "computeharbor"
    subprocess.run(
        ["go", "build", "-o", str(binary), "./control-plane/cmd/computeharbor"],
        cwd=repo_root,
        check=True,
    )

    preempt_flag = {"preempted": False}

    class _MockGCPMetadataHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.headers.get("Metadata-Flavor") != "Google":
                self.send_response(403)
                self.end_headers()
                return
            body = b"TRUE" if preempt_flag["preempted"] else b"FALSE"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    meta_server = ThreadingHTTPServer(("127.0.0.1", 0), _MockGCPMetadataHandler)
    meta_thread = threading.Thread(target=meta_server.serve_forever, daemon=True)
    meta_thread.start()
    meta_url = f"http://127.0.0.1:{meta_server.server_port}/computeMetadata/v1/instance/preempted"

    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    api_key = "cluster-e2e-secret"
    org_id = "org-cluster"
    job_id = "job-multi-node-1"

    server_proc = subprocess.Popen(
        [str(binary), "serve", "-addr", f"127.0.0.1:{port}", "-api-key", api_key],
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _wait_for_healthz(base_url)

        client = ControlPlaneClient(base_url, org_id=org_id, api_key=api_key)
        client.submit_job(
            job_id=job_id,
            gpu="nvidia-a100-40gb",
            target_steps=10,
            max_preemptions=2,
        )
        client.assign_worker(job_id, "spot-node-rank0")

        cfg = LoRAConfig(in_features=32, out_features=16, rank=4, base_seed=99)
        ckpt_dir = tmp_path / "ckpt"
        trainer = LoRATrainer(job_id=job_id, config=cfg, checkpoint_dir=ckpt_dir)
        partial = trainer.train(target_steps=5)
        assert partial.summary.completed_step == 5

        with ThreadPoolExecutor(max_workers=2) as pool:
            f0 = pool.submit(client.barrier, job_id, world_size=2, rank=0, step=5)
            f1 = pool.submit(client.barrier, job_id, world_size=2, rank=1, step=5)
            r0 = f0.result(timeout=5.0)
            r1 = f1.result(timeout=5.0)

        assert max(r0["committed_step"], r1["committed_step"]) == 5
        assert (r0["all_ready"] != r1["all_ready"]) is True

        watcher = MetadataPreemptionWatcher(
            provider="gcp",
            metadata_url=meta_url,
            poll_interval_sec=0.02,
        )
        watcher.start()
        try:
            preempt_flag["preempted"] = True
            assert watcher.wait(timeout=2.0) is True
        finally:
            watcher.stop()

        client.notify_sigterm(job_id)
        assert partial.manifest is not None
        migrating = client.commit_checkpoint(
            job_id=job_id,
            step=5,
            epoch=partial.manifest.epoch,
            loss=partial.summary.loss_history[-1],
            safetensors_uri=str(ckpt_dir / partial.manifest.safetensors_filename),
            manifest_uri=str(ckpt_dir / "latest.json"),
            flush_latency_ms=partial.summary.flush_duration_ms,
        )
        assert migrating["status"] == "MIGRATING"
        assert migrating["preemption_count"] == 1

        client.assign_worker(job_id, "spot-node-failover-1")
        resumed = LoRATrainer(job_id=job_id, config=cfg, checkpoint_dir=ckpt_dir)
        final = resumed.train(target_steps=10)
        assert final.summary.completed_step == 10
        assert final.manifest is not None

        done = client.commit_checkpoint(
            job_id=job_id,
            step=10,
            epoch=final.manifest.epoch,
            loss=final.summary.loss_history[-1],
            safetensors_uri=str(ckpt_dir / final.manifest.safetensors_filename),
            manifest_uri=str(ckpt_dir / "latest.json"),
            flush_latency_ms=final.summary.flush_duration_ms,
        )
        assert done["status"] == "COMPLETED"
        assert done["current_step"] == 10
    finally:
        meta_server.shutdown()
        meta_server.server_close()
        server_proc.terminate()
        server_proc.wait(timeout=5.0)
