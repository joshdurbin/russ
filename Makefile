BINARY := russ
MODULE := $(shell go list -m)
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -ldflags "-X main.version=$(VERSION)"

.PHONY: build clean test vet fmt

build:
	go build $(LDFLAGS) -o $(BINARY) .

clean:
	rm -f $(BINARY)

test:
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -w .
