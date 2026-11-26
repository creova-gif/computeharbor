VENV_BIN := .venv/bin
PYTEST := $(VENV_BIN)/pytest
MYPY := $(VENV_BIN)/mypy
RUFF := $(VENV_BIN)/ruff

BIN_DIR := bin
BINARY := $(BIN_DIR)/computeharbor

.PHONY: build test test-go test-py test-e2e lint bench chaos clean

build:
	@mkdir -p $(BIN_DIR)
	go build -o $(BINARY) ./control-plane/cmd/computeharbor

test: test-go test-py

test-go:
	go test -race ./...

test-py:
	$(PYTEST) worker/tests

test-e2e:
	$(PYTEST) tests/e2e

chaos:
	$(PYTEST) tests/chaos

lint:
	@unformatted="$$(gofmt -l pkg control-plane)"; \
	if [ -n "$$unformatted" ]; then \
		echo "Unformatted Go files:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi
	go vet ./...
	$(RUFF) check worker tests
	$(MYPY) --strict worker/src worker/tests tests/chaos tests/e2e

bench:
	go test -run=^$$ -bench=. -benchmem ./pkg/cost ./pkg/orchestrator

clean:
	rm -rf $(BIN_DIR) .pytest_cache .mypy_cache .ruff_cache
