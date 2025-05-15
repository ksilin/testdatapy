.PHONY: install install-dev test test-unit test-integration lint format type-check clean docker-up docker-down coverage test-with-coverage

install:
	uv pip install -e .

install-dev:
	uv pip install -e .[dev]

test: test-unit test-integration

test-unit:
	pytest tests/unit -v

test-integration: docker-up
	pytest tests/integration -v
	$(MAKE) docker-down

test-with-coverage:
	pytest --cov=src/testdatapy --cov-report=html --cov-report=term tests/

coverage:
	coverage run -m pytest tests/
	coverage report
	coverage html

lint:
	ruff check src tests

format:
	black src tests
	ruff check --fix src tests

type-check:
	mypy src

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .coverage
	rm -rf htmlcov
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf dist

docker-up:
	docker compose -f docker/docker-compose.yml up -d
	@echo "Waiting for Kafka to be ready..."
	@sleep 10

docker-down:
	docker-compose -f docker/docker-compose.yml down

dev-setup: install-dev
	pre-commit install

check: lint type-check test
