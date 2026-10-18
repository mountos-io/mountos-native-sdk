PROTOC ?= protoc
PROTOC_GEN_GO ?= $(HOME)/go/bin/protoc-gen-go
GEN_FLAGS = -spec ../api/filesystem.yaml -client ../api/client.yaml -snapshot ../api/contract-snapshot.json \
  -proto-out ../api/proto/mountos_fs.proto -go-out ../go/client -ts-out ../ts/src -rust-out ../rust/src

.PHONY: help api-check api-update api-gen gen-test ts-gen test

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

api-gen: ## Regenerate the proto, Go wire types, and client stubs for every language
	cd gen && go run . $(GEN_FLAGS)
	$(PROTOC) --plugin=protoc-gen-go=$(PROTOC_GEN_GO) --proto_path=api/proto --go_out=go \
	  --go_opt=module=github.com/mountos-io/mountos-native-sdk/go api/proto/mountos_fs.proto
	cd ts && npm run gen

api-check: ## Validate the specs, check compatibility, and fail when a generated file is stale
	cd gen && go run . $(GEN_FLAGS) -verify

api-update: ## Write the contract snapshot after validation and compatibility checks pass
	cd gen && go run . $(GEN_FLAGS) -update

gen-test: ## Run the generator's unit tests
	cd gen && go test ./...

test: gen-test ## Run every SDK's tests
	cd go && go test -race -count=1 ./...
	cd ts && npm test
	cd rust && cargo test --locked
