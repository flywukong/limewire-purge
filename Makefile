BIN := limewire-purge

# The greenfield-go-sdk dependency chain (cometbft -> blst) only compiles under
# Go 1.21.x, which is also the greenfield stack's standard build toolchain.
GOTOOLCHAIN := go1.21.13

.PHONY: build linux test test-logic

build:
	GOTOOLCHAIN=$(GOTOOLCHAIN) go build -o $(BIN) ./cmd/limewire-purge

# for the devops host (x86_64 Linux); copy the resulting binary with scp
linux:
	GOTOOLCHAIN=$(GOTOOLCHAIN) CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o $(BIN)-linux-amd64 ./cmd/limewire-purge

# full vet + test; use on the Linux build host (Go 1.21)
test:
	GOTOOLCHAIN=$(GOTOOLCHAIN) go vet ./... && GOTOOLCHAIN=$(GOTOOLCHAIN) go test ./...

# logic-only tests; portable to any Go version and any OS (no go-sdk/blst imports)
test-logic:
	go test ./internal/pieceop/ ./internal/purge/ ./internal/spconfig/
