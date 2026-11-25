# Contributing to ComputeHarbor

This guide covers setting up a local development environment, running tests and linters, and submitting pull requests.

## Set Up Your Environment

1. Install **Go 1.23+** and **Python 3.12+**.
2. Create a virtual environment at `.venv` and install the worker package with dev dependencies:

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
```

3. Verify the Go module and Python tooling:

```bash
go build ./...
make lint
```

## Run Checks Locally

Run static analysis across Go (`gofmt`, `go vet`) and Python (`ruff`, `mypy --strict`):

```bash
make lint
```

Run the Go and Python unit test suites:

```bash
make test
```

Run the spot-preemption chaos suite (`SIGTERM` interruption and bit-for-bit loss parity):

```bash
make chaos
```

## Submit a Pull Request

All contributions must follow the standards in [`AGENTS.md`](AGENTS.md):

1. **Architecture decisions:** Document non-trivial design changes in `docs/adr/` using [`docs/adr/000-template.md`](docs/adr/000-template.md) before or alongside implementation.
2. **Code quality:**
   - **Go:** Format with `gofmt`, pass `go vet ./...` and `go test -race ./...`, and handle all returned errors explicitly.
   - **Python:** Target Python 3.12+, use Pydantic v2 for data models, pass `ruff check` and `mypy --strict`, and never swallow errors with bare `except Exception: pass`.
3. **Tests:** Include deterministic unit tests for every behavior change. Do not report benchmark numbers without running them.
4. **Security:** Never commit `.env` files, API keys, or PII/PHI fixtures.
