GO=go
BUILD_PATH=./bin
GOLANGCI_LINT=$(BUILD_PATH)/golangci-lint
GOLANGCI_LINT_VERSION=v2.1.6

.PHONY: build clean test lint help

build:
	$(GO) build -o $(BUILD_PATH)/dberd ./cmd/dberd

clean:
	$(GO) clean
	rm -rf $(BUILD_PATH)

test:
	$(GO) test ./... -race -v -covermode=atomic -coverprofile=coverage.out

lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run

$(GOLANGCI_LINT):
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/$(GOLANGCI_LINT_VERSION)/install.sh | sh -s -- -b $(BUILD_PATH) $(GOLANGCI_LINT_VERSION)
