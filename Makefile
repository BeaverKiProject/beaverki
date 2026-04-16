CONFIG_DIR ?=

ifeq ($(strip $(CONFIG_DIR)),)
CONFIG_ARG :=
else
CONFIG_ARG := --config-dir $(CONFIG_DIR)
endif

.PHONY: fmt lint test check setup run-task show-task

fmt:
	cargo fmt --all

lint:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace

check:
	cargo check --workspace

setup:
	cargo run -p beaverki-cli -- setup init $(CONFIG_ARG)

verify-openai:
	cargo run -p beaverki-cli -- setup verify-openai

show-models:
	cargo run -p beaverki-cli -- setup show-models $(CONFIG_ARG)

set-models:
	cargo run -p beaverki-cli -- setup set-models $(CONFIG_ARG) $(MODEL_ARGS)

run-task:
	cargo run -p beaverki-cli -- task run $(CONFIG_ARG) --objective "$(OBJECTIVE)"

show-task:
	cargo run -p beaverki-cli -- task show $(CONFIG_ARG) --task-id $(TASK_ID)
