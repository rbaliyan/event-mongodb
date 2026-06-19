# Container runtime: docker or podman
DOCKER := env("DOCKER", "docker")

# MongoDB container settings
MONGO_CONTAINER := "event-mongodb-test"
MONGO_PORT := "27018"
MONGO_IMAGE := "mongo:6.0"

# Default recipe
default:
    @just --list

# Run all tests (unit + integration)
test: test-unit test-integration

# Run unit tests only
test-unit:
    go test -v -short ./...

# Fast hermetic smoke check (no MongoDB): build + short tests + examples
smoke:
    go build ./...
    go test -short -count=1 ./...
    go test -short -run Example ./...

# Run all benchmarks (no MongoDB required for the pure-logic hot paths)
bench:
    go test -run='^$' -bench=. -benchmem ./...

# Compare benchmarks against a base ref using benchstat (default: main)
bench-compare base="main":
    #!/usr/bin/env bash
    set -euo pipefail
    command -v benchstat >/dev/null || go install golang.org/x/perf/cmd/benchstat@latest
    go test -run='^$' -bench=. -benchmem -count=6 ./... > /tmp/new.txt
    git worktree add -f /tmp/bench-base {{base}}
    ( cd /tmp/bench-base && go test -run='^$' -bench=. -benchmem -count=6 ./... > /tmp/old.txt ) || true
    git worktree remove --force /tmp/bench-base
    benchstat /tmp/old.txt /tmp/new.txt

# Run a fuzz target for a duration (e.g. just fuzz FuzzBSONCodecDecode ./codec 30s)
fuzz target="FuzzConvertBSOND" pkg="." duration="30s":
    go test -run='^$' -fuzz='^{{target}}$' -fuzztime={{duration}} {{pkg}}

# Run integration tests (requires MongoDB)
test-integration: mongo-start
    #!/usr/bin/env bash
    set -euo pipefail
    echo "Waiting for MongoDB to be ready..."
    sleep 2
    MONGO_URI="mongodb://localhost:{{MONGO_PORT}}/?directConnection=true" go test -v -run Integration ./...
    just mongo-stop

# Start MongoDB replica set for testing
mongo-start:
    #!/usr/bin/env bash
    set -euo pipefail
    if {{DOCKER}} ps -a --format '{{{{.Names}}}}' | grep -q "^{{MONGO_CONTAINER}}$"; then
        echo "Container {{MONGO_CONTAINER}} already exists, removing..."
        {{DOCKER}} rm -f {{MONGO_CONTAINER}} > /dev/null
    fi
    echo "Starting MongoDB replica set..."
    {{DOCKER}} run -d --name {{MONGO_CONTAINER}} -p {{MONGO_PORT}}:27017 {{MONGO_IMAGE}} --replSet rs0
    echo "Waiting for MongoDB to start..."
    sleep 3
    echo "Initializing replica set..."
    {{DOCKER}} exec {{MONGO_CONTAINER}} mongosh --eval "rs.initiate()" > /dev/null
    echo "MongoDB replica set ready on port {{MONGO_PORT}}"

# Stop MongoDB container
mongo-stop:
    #!/usr/bin/env bash
    if {{DOCKER}} ps -a --format '{{{{.Names}}}}' | grep -q "^{{MONGO_CONTAINER}}$"; then
        echo "Stopping MongoDB container..."
        {{DOCKER}} stop {{MONGO_CONTAINER}} > /dev/null
        {{DOCKER}} rm {{MONGO_CONTAINER}} > /dev/null
        echo "MongoDB container stopped"
    else
        echo "MongoDB container not running"
    fi

# Show MongoDB logs
mongo-logs:
    {{DOCKER}} logs -f {{MONGO_CONTAINER}}

# Connect to MongoDB shell
mongo-shell:
    {{DOCKER}} exec -it {{MONGO_CONTAINER}} mongosh

# Build the project
build:
    go build ./...

# Run go mod tidy
tidy:
    go mod tidy

# Format code
fmt:
    go fmt ./...

# Run linter
lint:
    golangci-lint run ./...

# Clean up
clean: mongo-stop
    go clean -testcache

# Build examples
examples:
    go build -o bin/main examples/main.go

# Run vulnerability check
vulncheck:
    go run golang.org/x/vuln/cmd/govulncheck@latest ./...

# Check for outdated dependencies
depcheck:
    go list -m -u all | grep '\[' || echo "All dependencies are up to date"

# Create and push a new release tag (bumps patch version)
release:
    ./scripts/release.sh
