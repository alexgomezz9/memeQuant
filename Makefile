.PHONY: install test lint collector stats doctor replay rebuild

install:
	python -m pip install -e ".[dev,research]"

test:
	pytest

lint:
	ruff check src tests scripts

collector:
	memequant-collector

stats:
	memequant-stats

doctor:
	memequant-doctor

replay:
	memequant-replay

rebuild:
	memequant-replay --output data/rebuilt_normalized
