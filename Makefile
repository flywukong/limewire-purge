BIN := limewire-purge

.PHONY: build linux test

build:
	go build -o $(BIN) ./cmd/limewire-purge

# for the devops host (x86_64 Linux); copy the resulting binary with scp
linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o $(BIN)-linux-amd64 ./cmd/limewire-purge

test:
	go vet ./... && go test ./...
