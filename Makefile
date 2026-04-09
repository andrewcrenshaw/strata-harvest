.PHONY: check lint typecheck test-verify fix help

UV := $(HOME)/.local/bin/uv

## check: Run everything CI runs — must be green before committing
check: lint typecheck test-verify

## lint: ruff check + format check (mirrors CI lint job)
lint:
	$(UV) run ruff check .
	$(UV) run ruff format --check .

## typecheck: mypy (mirrors CI typecheck job)
typecheck:
	$(UV) run mypy src/

## test-verify: verification-marked tests (mirrors CI test-verification job)
test-verify:
	$(UV) run pytest -m verification --tb=short -q

## fix: auto-fix ruff lint and format issues
fix:
	$(UV) run ruff check --fix .
	$(UV) run ruff format .

## help: show this help
help:
	@grep -E '^## ' Makefile | sed 's/## /  /'
