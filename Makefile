BIN_DIR = ./bin
BIN_NAME = ws
BUILD_DATE ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS = -X main.date=$(BUILD_DATE)

.PHONY: install
install:
	GO111MODULE=on &&\
	 go install golang.org/x/tools/cmd/goimports@latest


.PHONY: test
test:
	@echo "Running tests..."
	go test -v ./...

.PHONY: bench
bench:
	@echo "Running benchmarks..."
	go test -bench=. -benchmem ./...

.PHONY: lint
lint:
	@echo "Running linters..."
	golangci-lint run --timeout=5m

.PHONY: fmt
fmt:
	@echo "Formatting code..."
	gofmt -w .
	goimports -w .

# Quality gate - comprehensive checks before commit/push
.PHONY: check
check:
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "Running comprehensive quality checks..."
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo ""
	@echo "1 - Formatting code..."
	@$(MAKE) fmt
	@echo "   ✅ Code formatted"
	@echo ""
	@echo "2 - Running go vet..."
	@go vet ./...
	@echo "   ✅ go vet passed"
	@echo ""
	@echo "3 - Running golangci-lint..."
	@$(MAKE) lint
	@echo "   ✅ Linting passed"
	@echo ""
	@echo "4 - Running unit tests..."
	@$(MAKE) test
	@echo "   ✅ Unit tests passed"
	@echo ""
	@echo "5 - Running tests with race detector..."
	@go test -race -v ./...
	@echo "   ✅ Race detector tests passed"
	@echo ""
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@echo "✅ All quality checks passed!"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Install Git hooks
.PHONY: install-hooks
install-hooks:
	@echo "Installing Git hooks..."
	@git config core.hooksPath .githooks
	@chmod +x .githooks/*
	@echo "✅ Git hooks installed"
	@echo ""
	@echo "Installed hooks:"
	@echo "  - pre-push: Comprehensive quality gate before pushing to main/develop"

# Uninstall Git hooks
.PHONY: uninstall-hooks
uninstall-hooks:
	@echo "Uninstalling Git hooks..."
	@git config --unset core.hooksPath
	@echo "✅ Git hooks uninstalled"


.PHONY: clean-build
clean-build:
	rm -rf ./bin/*

.PHONY: build
build:
	@echo "Verifying build for all packages..."
	@go build ./...
	@echo "✅ Build verification passed"