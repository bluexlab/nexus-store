TOPDIR := $(strip $(dir $(realpath $(lastword $(MAKEFILE_LIST)))))

CGO_ENABLED ?= 0
ifneq (,$(wildcard $(TOPDIR)/.env))
	include $(TOPDIR)/.env
	export
endif

comma:= ,
empty:=
space:= $(empty) $(empty)

bold := $(shell tput bold)
green := $(shell tput setaf 2)
sgr0 := $(shell tput sgr0)

MODULE_NAME := $(shell go list -m)

PLATFORM ?= $(platform)
ifneq ($(PLATFORM),)
	GOOS := $(or $(word 1, $(subst /, ,$(PLATFORM))),$(shell go env GOOS))
	GOARCH := $(or $(word 2, $(subst /, ,$(PLATFORM))),$(shell go env GOARCH))
endif

BIN_SUFFIX :=
ifneq ($(or $(GOOS),$(GOARCH)),)
	GOOS ?= $(shell go env GOOS)
	GOARCH ?= $(shell go env GOARCH)
	BIN_SUFFIX := $(BIN_SUFFIX)-$(GOOS)-$(GOARCH)
endif
ifeq ($(GOOS),windows)
	BIN_SUFFIX := $(BIN_SUFFIX).exe
endif

APPS := $(patsubst app/%/,%,$(sort $(dir $(wildcard app/*/))))
GOFILES := $(shell find . -type f -name '*.go' -not -path '*/\.*' -not -path './app/*')
$(foreach app,$(APPS),\
	$(eval GOFILES_$(app) := $(shell find ./app/$(app) -type f -name '*.go' -not -path '*/\.*')))

PROTO_DIR := proto
PROTO_SOURCES := $(shell find $(PROTO_DIR) -type f -name '*.proto' -not -path '*/\.*')
PROTO_FILES := $(patsubst $(PROTO_DIR)/%,%,$(PROTO_SOURCES))
PROTO_GRPC_SOURCES := $(shell grep -slrw $(PROTO_DIR) -e '^service')
PROTO_GRPC_FILES := $(patsubst $(PROTO_DIR)/%,%,$(PROTO_GRPC_SOURCES))

PROTO_GO_DIR := pkg/proto
PROTO_GO_FILES := $(patsubst %.proto,$(PROTO_GO_DIR)/%.pb.go,$(PROTO_FILES))
PROTO_GO_GRPC_FILES := $(patsubst %.proto,$(PROTO_GO_DIR)/%_grpc.pb.go,$(PROTO_GRPC_FILES))

PROTO_GO_OPTS := $(patsubst %,M%,$(join $(PROTO_FILES),$(patsubst %/,=$(MODULE_NAME)/%,$(dir $(PROTO_GO_FILES)))))
PROTO_GO_OUT_OPT := $(subst $(space),$(comma),$(PROTO_GO_OPTS))

SQLCFILES := $(shell find ./pkg/dbaccess/dbsqlc -type f -name '*.sql')
MIGRATIONFILES := $(shell find ./db/migration -type f -name '*.sql')
GENERATED_SQLCFILES := $(SQLCFILES:.sql=.sql.go)

.DEFAULT: all

.PHONY: all
all: $(APPS)

.PHONY: $(APPS)
$(APPS): %: bin/%$(BIN_SUFFIX)

.PHONY: pb-fmt
pb-fmt:
	@$(MAKE) -C $(PROTO_DIR) fmt

.PHONY: pb
pb: $(PROTO_GO_FILES) $(PROTO_GO_GRPC_FILES)

.PHONY: generate
generate: $(GENERATED_SQLCFILES)

$(GENERATED_SQLCFILES): $(SQLCFILES) $(MIGRATIONFILES)
	@echo "Generating SQLC code..."
	@cd ./pkg/dbaccess/dbsqlc && go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.27.0 generate

$(PROTO_GO_DIR)/%.pb.go: $(PROTO_DIR)/%.proto
	@mkdir -p $(@D)
	@printf "Generating $(bold)$@$(sgr0) ... "
	@protoc -I $(PROTO_DIR) \
		--go_out=$(PROTO_GO_OUT_OPT):$(PROTO_GO_DIR) \
		--go_opt=paths=source_relative \
		$<
	@printf "$(green)done$(sgr0)\n"

$(PROTO_GO_DIR)/%_grpc.pb.go: $(PROTO_DIR)/%.proto
	@mkdir -p $(@D)
	@printf "Generating $(bold)$@$(sgr0) ... "
	@protoc -I $(PROTO_DIR) \
		--go-grpc_out=$(PROTO_GO_OUT_OPT):$(PROTO_GO_DIR) \
		--go-grpc_opt=paths=source_relative \
		$<
	@printf "$(green)done$(sgr0)\n"

.SECONDEXPANSION:
bin/%: $$(GOFILES) $$(GOFILES_$$(@F)) $$(ASSET_FILES) $(GENERATED_API_SERVER) $(GENERATED_SQLCFILES) $$(PROTO_GO_FILES) $$(PROTO_GO_GRPC_FILES)
	@printf "Building $(bold)$@$(sgr0) ... "
	@go build -o ./bin/$(@F) ./app/$(@F:$(BIN_SUFFIX)=)
	@printf "$(green)done$(sgr0)\n"

.PHONY: gosec
gosec: ## Run the golang security checker
	@gosec -exclude-dir test/mock ./...

.PHONY: test
test: ## Run unit test
	@go clean -testcache
	@go test ./...

.PHONY: coverage
coverage:
	@go clean -testcache
	@go test -p 1 -v -cover -covermode=count -coverprofile=coverage.out ./...
	@go tool cover -html coverage.out -o coverage.html
	@go tool cover -func coverage.out | tail -n 1
