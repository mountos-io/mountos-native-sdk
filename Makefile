GOBIN_DIR ?= $(HOME)/go/bin
GO_TOOLS = PATH="$(GOBIN_DIR):$$PATH"
CARGO_BIN_DIR ?= $(HOME)/.cargo/bin
PROTOC ?= protoc
PROTOC_GEN_GO ?= $(HOME)/go/bin/protoc-gen-go
GEN_FLAGS = -spec ../api/filesystem.yaml -client ../api/client.yaml -snapshot ../api/contract-snapshot.json \
  -proto-out ../api/proto/mountos_fs.proto -go-out ../go/client -ts-out ../ts/src -rust-out ../rust/src
SPEC_FLAGS = -spec ../api/filesystem.yaml -client ../api/client.yaml -snapshot ../api/contract-snapshot.json

.PHONY: all help hooks api-check api-update api-gen gen-test proto-check proto-coherence version-check \
  check vet test build clean install lint vuln audit \
  ts-install ts-check ts-lint ts-build ts-test ts-audit ts-publish ts-clean \
  go-check go-build go-test \
  rust-check rust-fmt rust-audit rust-build rust-test rust-publish rust-clean \
  tag tag-minor tag-major bump-version tag-guard

all: check build

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

hooks: ## Install the git hooks (lefthook)
	lefthook install

# ── Generator ───────────────────────────────────────────────

api-gen: ## Regenerate the proto, Go wire types, and client stubs for every language
	cd gen && go run . $(GEN_FLAGS)
	$(PROTOC) --plugin=protoc-gen-go=$(PROTOC_GEN_GO) --proto_path=api/proto --go_out=go \
	  --go_opt=module=github.com/mountos-io/mountos-native-sdk/go api/proto/mountos_fs.proto
	cp api/proto/mountos_fs.proto rust/proto/mountos_fs.proto
	cd ts && npm run gen

api-check: ## Validate the specs, check compatibility, and fail when a generated file is stale
	cd gen && go run . $(GEN_FLAGS) -verify

api-update: ## Write the contract snapshot after validation and compatibility checks pass
	cd gen && go run . $(GEN_FLAGS) -update

gen-test: ## Run the generator's unit tests (includes the proto descriptor coherence test)
	cd gen && go test ./...

# ── Drift and coherence ─────────────────────────────────────

proto-check: ## Fail when go/wire or ts/src/gen differs from protoc output for the committed proto
	./scripts/proto-check.sh

proto-coherence: ## Fail when the committed proto differs from the spec (text and descriptor level)
	cd gen && go run . $(SPEC_FLAGS) -proto-out ../api/proto/mountos_fs.proto -verify \
	  || { echo "proto-coherence: api/proto/mountos_fs.proto does not match api/filesystem.yaml. Fix: make api-gen"; exit 1; }
	cmp -s api/proto/mountos_fs.proto rust/proto/mountos_fs.proto \
	  || { echo "proto-coherence: rust/proto/mountos_fs.proto differs from api/proto/mountos_fs.proto. Fix: make api-gen"; exit 1; }
	cd gen && go test -count=1 -run 'TestCommittedProto|TestDescriptorCheck' ./... \
	  || { echo "proto-coherence: descriptor check failed. Fix: make api-gen"; exit 1; }

version-check: ## Check npm and crate versions match and the spec minorVersion is not below the snapshot
	./scripts/version-check.sh
	cd gen && go run . $(SPEC_FLAGS) \
	  || { echo "version-check: spec fails the snapshot compatibility rules. Bump minorVersion or restore the contract, then run make api-update"; exit 1; }

# ── TypeScript ──────────────────────────────────────────────

ts-install: ## Install TS dependencies
	cd ts && npm install --no-audit --no-fund --prefer-offline

ts-check: ts-install ## Type-check TS
	cd ts && npm run typecheck

ts-lint: ts-install ## Lint TS with ESLint (type-aware rules)
	cd ts && npm run lint

ts-audit: ts-install ## Audit runtime npm dependencies (needs network, fails on known advisories)
	cd ts && npm audit --omit=dev

ts-build: ts-install ## Build TS
	cd ts && npm run build

ts-test: ts-install ## Run TS tests
	cd ts && npm test

ts-publish: ts-build ## Publish TS package to npm
	@npm whoami >/dev/null 2>&1 || npm login
	cd ts && npm publish --access public

ts-clean: ## Remove TS build artifacts
	rm -rf ts/dist ts/node_modules

# ── Go ──────────────────────────────────────────────────────

go-check: ## Vet Go code (client module and generator)
	cd go && go vet ./...
	cd gen && go vet ./...

lint: ## Run golangci-lint on the Go client module and the generator
	@$(GO_TOOLS) command -v golangci-lint >/dev/null 2>&1 || { echo "lint: golangci-lint not found in PATH or $(GOBIN_DIR)"; exit 1; }
	cd go && $(GO_TOOLS) golangci-lint run --config ../.golangci.yml ./...
	cd gen && $(GO_TOOLS) golangci-lint run --config ../.golangci.yml ./...

vuln: ## Fail when Go code calls a symbol with a known vulnerability (govulncheck)
	@$(GO_TOOLS) command -v govulncheck >/dev/null 2>&1 || { echo "vuln: govulncheck not found in PATH or $(GOBIN_DIR)"; exit 1; }
	cd go && $(GO_TOOLS) govulncheck ./...
	cd gen && $(GO_TOOLS) govulncheck ./...

go-build: ## Build Go code
	cd go && go build ./...
	cd gen && go build ./...

go-test: ## Run Go client tests
	cd go && go test -race -count=1 ./...

# ── Rust ────────────────────────────────────────────────────

rust-check: ## Clippy-lint Rust code (warnings are errors); builds against the committed proto
	cd rust && cargo clippy --all-targets --locked -- -D warnings

rust-fmt: ## Check Rust formatting (rustfmt.toml)
	cd rust && cargo fmt --check

rust-audit: ## Audit crate dependencies with cargo audit (fails on advisories, skips when not installed)
	@if PATH="$(CARGO_BIN_DIR):$$PATH" cargo audit --version >/dev/null 2>&1; then cd rust && PATH="$(CARGO_BIN_DIR):$$PATH" cargo audit --deny warnings; else echo "rust-audit: cargo-audit is not installed, skipping (install: cargo install cargo-audit)"; fi

rust-build: ## Build Rust crate
	cd rust && cargo build --locked

rust-test: ## Run Rust unit + doc tests
	cd rust && cargo test --locked

rust-publish: rust-build ## Publish Rust crate to crates.io
	cd rust && cargo publish

rust-clean: ## Remove Rust build artifacts
	cd rust && cargo clean

# ── Combined ────────────────────────────────────────────────

install: ts-install ## Install all dependencies

vet: go-check lint rust-fmt rust-check ts-check ts-lint ## go vet, golangci-lint, cargo fmt --check, clippy -D warnings, tsc --noEmit, eslint

check: vet vuln rust-audit api-check proto-check proto-coherence version-check ## Vet, govulncheck, cargo audit, plus every drift and coherence check

audit: ts-audit rust-audit ## Dependency audits (npm audit needs network, cargo audit also runs in check)

test: gen-test go-test ts-test rust-test ## Run every SDK's tests

build: ts-build go-build rust-build ## Build all

clean: ts-clean rust-clean ## Clean all artifacts

# ── Version ────────────────────────────────────────────────

VERSION := $(shell jq -r .version ts/package.json)
MAJOR   := $(word 1,$(subst ., ,$(VERSION)))
MINOR   := $(word 2,$(subst ., ,$(VERSION)))
PATCH   := $(word 3,$(subst ., ,$(VERSION)))

tag: tag-minor ## Alias for tag-minor

# Refuses to tag from a dirty tree or when the checks and tests fail.
tag-guard:
	@test -z "$$(git status --porcelain)" || { echo "tag: git tree is not clean, commit or stash first"; exit 1; }
	@$(MAKE) --no-print-directory check || { echo "tag: make check failed, not tagging"; exit 1; }
	@$(MAKE) --no-print-directory test || { echo "tag: make test failed, not tagging"; exit 1; }
	@test -z "$$(git status --porcelain)" || { echo "tag: checks changed the git tree, not tagging"; exit 1; }

# bump-version writes $(NEW) into the npm and crates.io manifests, keeping them
# in lockstep. The Go module lives in go/, so its version is the go/v* git tag.
# Cargo.lock is refreshed (--workspace touches only our package, not deps) so
# `--locked` builds keep working after the bump.
bump-version: tag-guard
	@jq '.version = "$(NEW)"' ts/package.json > ts/package.json.tmp && mv ts/package.json.tmp ts/package.json
	@jq '.version = "$(NEW)" | .packages[""].version = "$(NEW)"' ts/package-lock.json > ts/package-lock.json.tmp && mv ts/package-lock.json.tmp ts/package-lock.json
	@awk -v v="$(NEW)" '/^\[/{p=($$0=="[package]")} p && /^version = /{print "version = \"" v "\""; next} {print}' rust/Cargo.toml > rust/Cargo.toml.tmp && mv rust/Cargo.toml.tmp rust/Cargo.toml
	@cd rust && cargo update --workspace --offline
	@git add .
	@git commit -m "v$(NEW)"
	@git tag "v$(NEW)"
	@git tag "go/v$(NEW)"
	@git push origin --tags $(shell git branch --show-current)
	@echo "$(VERSION) → $(NEW)"

tag-minor: ## Bump minor version, commit and tag
	@$(MAKE) bump-version NEW=$(MAJOR).$(shell echo $$(($(MINOR)+1))).0

tag-major: ## Bump major version, commit and tag
	@$(MAKE) bump-version NEW=$(shell echo $$(($(MAJOR)+1))).0.0
