BIN := limewire-purge

# The greenfield-go-sdk dependency chain (cometbft -> blst / herumi-bls) only
# compiles under Go 1.21.x, the greenfield stack's standard build toolchain,
# and it needs CGO (bls/blst are C libraries).
GOTOOLCHAIN := go1.21.13

.PHONY: build linux test test-logic

build:
	GOTOOLCHAIN=$(GOTOOLCHAIN) CGO_ENABLED=1 go build -o $(BIN) ./cmd/limewire-purge

# Cross-compiling needs CGO, so a plain GOOS=linux build from macOS will NOT
# link (undefined blst/bls symbols). Two ways to get a linux/amd64 binary:
#   1. Simplest: clone the repo on the linux/amd64 host and run `make build`.
#   2. Cross-compile with a C cross-toolchain, e.g. zig:
#        make linux CC="zig cc -target x86_64-linux-gnu"
# `go build` reads CC from the environment; set it before running this target.
linux:
	GOTOOLCHAIN=$(GOTOOLCHAIN) CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -trimpath -o $(BIN)-linux-amd64 ./cmd/limewire-purge

# full vet + test; run on the Linux build host (Go 1.21, CGO available)
test:
	GOTOOLCHAIN=$(GOTOOLCHAIN) go vet ./... && GOTOOLCHAIN=$(GOTOOLCHAIN) go test ./...

# logic-only tests; portable to any Go version and any OS (no go-sdk/blst imports)
test-logic:
	go test ./internal/pieceop/ ./internal/purge/ ./internal/spconfig/
