test:
	uv run pytest -vv

deps:
	uv sync --dev 

check:
	uv run pyright && uv run ruff check --fix