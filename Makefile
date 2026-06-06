# Makefile

MAKEFLAGS += -j2
-include .env
export

CURRENT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
CURRENT_PATH := $(shell pwd)
DEFAULT_BRANCH := $(shell git remote show upstream | sed -n '/HEAD branch/s/.*: //p')
AMM := ${HOME}/amm

.PHONY: gitRebase
gitRebase:
	git checkout $(DEFAULT_BRANCH) && \
		git pull upstream $(DEFAULT_BRANCH) && \
		git push origin $(DEFAULT_BRANCH) && \
		git checkout $(CURRENT_BRANCH) && \
		git rebase $(DEFAULT_BRANCH)
		git push --force origin $(CURRENT_BRANCH)

.PHONY: gitAmmend
gitAmmend:
	git add . && git commit --amend --no-edit && git push --force origin $(CURRENT_BRANCH)

.PHONY: test
test:
	cargo watch -qcx 'test'

.PHONY: coverage
coverage:
	cargo llvm-cov --html

.PHONY: pgadmin
pgadmin:
	docker run -p 5050:80 \
    -e 'PGADMIN_DEFAULT_EMAIL=user@domain.com' \
    -e 'PGADMIN_DEFAULT_PASSWORD=SuperSecret' \
    -d dpage/pgadmin4

.PHONY: fix
fix:
	cargo clippy --fix

.PHONY: install-hooks
install-hooks:
	git config core.hooksPath .githooks
	chmod +x .githooks/*
	@echo "Git hooks installed (core.hooksPath -> .githooks)."

.PHONY: uninstall-hooks
uninstall-hooks:
	git config --unset core.hooksPath
	@echo "Git hooks uninstalled."

.PHONY: wasm-test
wasm-test:
	wasm-pack test --headless --chrome macros-tests
