pre-commit run --all-files

mypy --show-error-codes --show-error-context epochai/

ruff check --init-config
