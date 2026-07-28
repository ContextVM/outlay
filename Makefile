# outlay — release tooling.
#
# `make patch|minor|major` bumps the shared version across all crates, commits,
# tags `v<ver>`, and pushes — the tag push triggers .github/workflows/release.yml
# which builds the linux amd64/arm64 binaries and the multi-arch Docker image.

.PHONY: patch minor major release version help

CRATES := $(wildcard crates/*/Cargo.toml)

help: ## show available targets
	@grep -hE '^[a-zA-Z _-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n",$$1,$$2}'

version: ## print the current shared version
	@awk -F'"' '/^version = /{print $$2; exit}' crates/outlay/Cargo.toml

release: ## tag the CURRENT version (no bump) + push — for the inaugural or a re-release
	@set -e; \
	CUR=$$(awk -F'"' '/^version = /{print $$2; exit}' crates/outlay/Cargo.toml); \
	[ -z "$$(git status --porcelain)" ] || { echo "working tree dirty; commit first" >&2; exit 1; }; \
	git rev-parse -q --verify "refs/tags/v$$CUR" >/dev/null && { echo "tag v$$CUR already exists" >&2; exit 1; }; \
	echo "tag v$$CUR (current version, no bump)"; \
	git tag "v$$CUR" && git push --tags; \
	echo "tagged v$$CUR — CI is building binaries + Docker images"

patch minor major: ## cut a release: bump version, commit, tag v<ver>, push (CI builds artifacts)
	@set -e; \
	LVL=$@; \
	CUR=$$(awk -F'"' '/^version = /{print $$2; exit}' crates/outlay/Cargo.toml); \
	MAJ=$${CUR%%.*}; REST=$${CUR#*.}; MIN=$${REST%%.*}; PAT=$${REST##*.}; \
	case "$$LVL" in \
	  major) MAJ=$$((MAJ+1)); MIN=0; PAT=0 ;; \
	  minor) MIN=$$((MIN+1)); PAT=0 ;; \
	  patch) PAT=$$((PAT+1)) ;; \
	esac; \
	NEW="$$MAJ.$$MIN.$$PAT"; \
	[ -z "$$(git status --porcelain)" ] || { echo "working tree dirty; commit or stash first" >&2; exit 1; }; \
	echo "bump $$CUR -> $$NEW ($$LVL)"; \
	for f in $(CRATES); do sed -i "s/^version = \"$$CUR\"/version = \"$$NEW\"/" $$f; done; \
	cargo check --workspace; \
	git add -A && git commit -q -m "chore: release v$$NEW" && git tag "v$$NEW"; \
	git push && git push --tags; \
	echo "released v$$NEW — CI is building binaries + Docker image"
