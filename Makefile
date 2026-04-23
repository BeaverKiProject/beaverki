CONFIG_DIR ?=

ifeq ($(strip $(CONFIG_DIR)),)
CONFIG_ARG :=
else
CONFIG_ARG := --config-dir $(CONFIG_DIR)
endif

.PHONY: fmt lint test check setup verify-openai show-models set-models daemon-start daemon-run daemon-status daemon-stop web-ui run-task show-task user-list user-add role-list approval-list approval-approve approval-deny package-release

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

daemon-start:
	cargo run -p beaverki-cli -- daemon start $(CONFIG_ARG)

daemon-run:
	cargo run -p beaverki-cli -- daemon run $(CONFIG_ARG)

daemon-status:
	cargo run -p beaverki-cli -- daemon status $(CONFIG_ARG)

daemon-stop:
	cargo run -p beaverki-cli -- daemon stop $(CONFIG_ARG)

web-ui:
	cargo run -p beaverki-web -- $(CONFIG_ARG)

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

ifndef RELEASE_TAG
package-release:
	$(error RELEASE_TAG is required, e.g. make package-release RELEASE_TAG=2026-04-23.1 PLATFORM_ID=linux-x86_64)
else ifndef PLATFORM_ID
package-release:
	$(error PLATFORM_ID is required, e.g. make package-release RELEASE_TAG=2026-04-23.1 PLATFORM_ID=linux-x86_64)
else
package-release:
	cargo build --release -p beaverki-cli -p beaverki-web
	bash packaging/build-release-archive.sh "$(RELEASE_TAG)" "$(PLATFORM_ID)"
endif
