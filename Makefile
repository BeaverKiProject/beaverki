CONFIG_DIR ?=

ifeq ($(strip $(CONFIG_DIR)),)
CONFIG_ARG :=
else
CONFIG_ARG := --config-dir $(CONFIG_DIR)
endif

.PHONY: fmt lint test check setup verify-openai show-models set-models run-task show-task user-list user-add role-list approval-list approval-approve approval-deny

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
	cargo run -p beaverki-cli -- task run $(CONFIG_ARG) $(TASK_ARGS) --objective "$(OBJECTIVE)"

show-task:
	cargo run -p beaverki-cli -- task show $(CONFIG_ARG) $(TASK_ARGS) --task-id $(TASK_ID)

user-list:
	cargo run -p beaverki-cli -- user list $(CONFIG_ARG)

user-add:
	cargo run -p beaverki-cli -- user add $(CONFIG_ARG) --display-name "$(DISPLAY_NAME)" $(USER_ARGS)

role-list:
	cargo run -p beaverki-cli -- role list $(CONFIG_ARG)

approval-list:
	cargo run -p beaverki-cli -- approval list $(CONFIG_ARG) $(APPROVAL_ARGS)

approval-approve:
	cargo run -p beaverki-cli -- approval approve $(CONFIG_ARG) $(APPROVAL_ARGS) --approval-id $(APPROVAL_ID)

approval-deny:
	cargo run -p beaverki-cli -- approval deny $(CONFIG_ARG) $(APPROVAL_ARGS) --approval-id $(APPROVAL_ID)
